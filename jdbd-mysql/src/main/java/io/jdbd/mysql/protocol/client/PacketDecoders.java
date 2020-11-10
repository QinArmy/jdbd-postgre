package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.conf.Properties;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.List;
import java.util.function.BiFunction;

public abstract class PacketDecoders {


    protected PacketDecoders() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }


    @Nullable
    public static ByteBuf packetDecoder(final ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        if (readableBytes < PacketUtils.HEADER_SIZE) {
            return null;
        }

        final int packetLength;
        packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex());

        final ByteBuf packetBuf;
        if (readableBytes < packetLength) {
            packetBuf = null;
        } else if (readableBytes == packetLength) {
            packetBuf = cumulateBuffer;
        } else {
            packetBuf = cumulateBuffer.readRetainedSlice(packetLength);
        }
        return packetBuf;
    }


    @Nullable
    public static ByteBuf comQueryResponseDecoder(final ByteBuf cumulateBuf, final int negotiatedCapability) {
        if (!PacketUtils.hasOnePacket(cumulateBuf)) {
            return null;
        }
        ByteBuf decodedByteBuf;
        final ComQueryResponse responseType = detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        switch (responseType) {
            case OK:
            case ERROR:
            case LOCAL_INFILE_REQUEST:
                decodedByteBuf = packetDecoder(cumulateBuf);
                break;
            case TEXT_RESULT:
                decodedByteBuf = comQueryTextResultSetMetadataDecoder(cumulateBuf, negotiatedCapability);
                break;
            default:
                throw new IllegalStateException(String.format("unknown ComQueryResponse[%s]", responseType));
        }
        return decodedByteBuf;
    }


    /*################################## blow package method ##################################*/

    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuf, final int negotiatedCapability) {
        int readerIndex = cumulateBuf.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1(cumulateBuf, readerIndex++)) {
            case 0:
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuf, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
                break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case PacketUtils.LOCAL_INFILE_REQUEST_HEADER:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    static MySQLColumnMeta[] readResultColumnMetas(ByteBuf columnMetaPacket, final int negotiatedCapability
            , Charset metaCharset, Properties properties) {

        columnMetaPacket.skipBytes(PacketUtils.HEADER_SIZE); //skip header

        final byte metadataFollows;
        final boolean hasOptionalMeta = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;
        if (hasOptionalMeta) {
            metadataFollows = columnMetaPacket.readByte();
        } else {
            metadataFollows = -1;
        }
        final int columnCount = PacketUtils.readLenEncAsInt(columnMetaPacket);
        MySQLColumnMeta[] columnMetas = new MySQLColumnMeta[columnCount];

        if (!hasOptionalMeta || metadataFollows == 1) {
            for (int i = 0; i < columnCount; i++) {
                columnMetaPacket.skipBytes(PacketUtils.HEADER_SIZE); // skip header
                columnMetas[i] = MySQLColumnMeta.readFor41(columnMetaPacket, metaCharset, properties);
            }
        }
        if ((negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0) {
            EofPacket.readPacket(columnMetaPacket, negotiatedCapability);
        }
        return columnMetas;
    }


    /**
     * @param resultList a modifiable and emtpy list
     * @return true :decode end.
     * @see MySQLCumulateReceiver#receive(BiFunction)
     */
    static boolean resultSetMultiRowDecoder(final ByteBuf cumulateBuf, List<ByteBuf> resultList) {
        final int originalReaderIndex = cumulateBuf.readerIndex(), writeIndex = cumulateBuf.writerIndex();

        int readerIndex = originalReaderIndex;
        boolean decoderEnd = false;

        for (int readableBytes, packetLength; ; ) {
            readableBytes = writeIndex - readerIndex;
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            packetLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
            if (readableBytes < PacketUtils.HEADER_SIZE + packetLength) {
                break;
            }
            switch (PacketUtils.getInt1(cumulateBuf, readerIndex + PacketUtils.HEADER_SIZE)) {
                case OkPacket.OK_HEADER:
                case EofPacket.EOF_HEADER:
                case ErrorPacket.ERROR_HEADER:
                    // The row data END
                    decoderEnd = true;
                    break;
                default:
                    readerIndex += packetLength;
            }
        }
        if (readerIndex > originalReaderIndex) {
            if (readerIndex == writeIndex) {
                resultList.add(cumulateBuf);
            } else {
                resultList.add(cumulateBuf.readRetainedSlice(readerIndex - originalReaderIndex));
            }
        }
        return decoderEnd;
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #comQueryResponseDecoder(ByteBuf, int)
     */
    @Nullable
    private static ByteBuf comQueryTextResultSetMetadataDecoder(final ByteBuf cumulateBuf
            , final int negotiatedCapability) {

        if (cumulateBuf.readableBytes() < 9) {
            return null;
        }
        final int startReaderIndex = cumulateBuf.readerIndex();

        cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE);
        // 1. metadata_follows
        final byte metadataFollows;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            // 0: RESULTSET_METADATA_NONE , 1:RESULTSET_METADATA_FULL
            metadataFollows = cumulateBuf.readByte();
        } else {
            metadataFollows = 1;
        }
        // 2. column_count
        final int columnCount = (int) PacketUtils.readLenEnc(cumulateBuf);
        int actualColumnCount = 0;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0 || metadataFollows == 1) {
            // 3. Field metadata
            for (; actualColumnCount < columnCount; actualColumnCount++) {
                int readableBytes = cumulateBuf.readableBytes();
                if (readableBytes < PacketUtils.HEADER_SIZE) {
                    break;
                }
                int packetLength;
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuf, cumulateBuf.readerIndex());
                if (readableBytes < packetLength) {
                    break;
                }
                cumulateBuf.skipBytes(packetLength);
            }
        } else {
            // reset reader index
            cumulateBuf.readerIndex(startReaderIndex);
            throw new JdbdMySQLException("COM_QUERY response packet,not present field metadata.");
        }

        if (actualColumnCount == columnCount && (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            // 4. End of metadata
            int readableBytes = cumulateBuf.readableBytes();

            if (readableBytes > PacketUtils.HEADER_SIZE) {
                int packetLength;
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuf, cumulateBuf.readerIndex());
                if (readableBytes >= packetLength && EofPacket.isEofPacket(cumulateBuf)) {
                    cumulateBuf.skipBytes(packetLength);
                } else {
                    // reset reader index
                    cumulateBuf.readerIndex(startReaderIndex);
                    throw new JdbdMySQLException("COM_QUERY response packet,End of metadata isn't EOF packet.");
                }

            }
        }

        // below obtain decodedBuf
        final int endReaderIndex = cumulateBuf.readerIndex();
        // reset reader index
        cumulateBuf.readerIndex(startReaderIndex);
        ByteBuf decodedBuf;
        if (actualColumnCount == columnCount) {
            if (endReaderIndex == cumulateBuf.writerIndex()) {
                decodedBuf = cumulateBuf;
            } else {
                decodedBuf = cumulateBuf.readRetainedSlice(endReaderIndex - startReaderIndex);
            }
        } else {
            decodedBuf = null;
        }
        return decodedBuf;
    }


}
