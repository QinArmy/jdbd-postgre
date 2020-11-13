package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.conf.Properties;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.MonoSink;

import java.nio.charset.Charset;
import java.util.function.BiFunction;

public abstract class PacketDecoders {


    protected PacketDecoders() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(PacketDecoders.class);


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }


    public static boolean packetDecoder(final ByteBuf cumulateBuffer, MonoSink<ByteBuf> sink) {
        final int readableBytes = cumulateBuffer.readableBytes();
        boolean decodeEnd;
        if (readableBytes < PacketUtils.HEADER_SIZE) {
            decodeEnd = false;
        } else {
            final int packetLength;
            packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex());
            if (readableBytes < packetLength) {
                decodeEnd = false;
            } else {
                decodeEnd = true;
                sink.success(cumulateBuffer.readRetainedSlice(packetLength));
            }
        }
        return decodeEnd;
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
            for (int i = 0, payloadLen, payloadIndex; i < columnCount; i++) {
                payloadLen = PacketUtils.readInt3(columnMetaPacket);
                columnMetaPacket.skipBytes(1); // skip sequence id.
                payloadIndex = columnMetaPacket.readerIndex();

                columnMetas[i] = MySQLColumnMeta.readFor41(columnMetaPacket, metaCharset, properties);
                columnMetaPacket.readerIndex(payloadIndex + payloadLen); // to next packet.
            }
        }
        if ((negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            columnMetaPacket.skipBytes(PacketUtils.HEADER_SIZE); //skip header
            EofPacket.readPacket(columnMetaPacket, negotiatedCapability);
        }
        return columnMetas;
    }


    /**
     * @return true :decode end.
     * @see MySQLCumulateReceiver#receive(BiFunction)
     */
    static boolean resultSetMultiRowDecoder(final ByteBuf cumulateBuf, FluxSink<ByteBuf> sink
            , final int negotiatedCapability) {
        final int originalReaderIndex = cumulateBuf.readerIndex(), writeIndex = cumulateBuf.writerIndex();
        final boolean clientDeprecateEof = (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        int readerIndex = originalReaderIndex;
        boolean decoderEnd = false;

        out:
        for (int readableBytes, packetLength, payloadLength, header; ; ) {
            readableBytes = writeIndex - readerIndex;
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
            packetLength = PacketUtils.HEADER_SIZE + payloadLength;
            if (readableBytes < packetLength) {
                break;
            }
            // LOG.debug("i:{}", i);
            header = PacketUtils.getInt1(cumulateBuf, readerIndex + PacketUtils.HEADER_SIZE);
            readerIndex += packetLength;
            switch (header) {
                case ErrorPacket.ERROR_HEADER:
                    // The row data END
                    decoderEnd = true;
                    break out;
                case EofPacket.EOF_HEADER:
                    if (clientDeprecateEof) {
                        if (payloadLength < 0xFFFF_FF) { // 3 byte max value
                            //OK terminator
                            decoderEnd = true;
                            break out;
                        }
                    } else if (packetLength < 10) {
                        // EOF terminator
                        decoderEnd = true;
                        break out;
                    }

                default:

            }
        }
        if (readerIndex > originalReaderIndex) {
            sink.next(cumulateBuf.readRetainedSlice(readerIndex - originalReaderIndex));
        }
        return decoderEnd;
    }

    /**
     * @see AbstractClientProtocol#comQueryResultSetMetaDecoder(ByteBuf, MonoSink)
     */
    static boolean comQueryResultSetMetadataDecoder(final ByteBuf cumulateBuf, MonoSink<ByteBuf> sink
            , final int negotiatedCapability) {

        if (cumulateBuf.readableBytes() < 9) {
            return false;
        }
        final int startReaderIndex = cumulateBuf.readerIndex();

        cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE);
        // 1. metadata_follows
        final byte metadataFollows;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            // 0: RESULTSET_METADATA_NONE , 1:RESULTSET_METADATA_FULL
            metadataFollows = cumulateBuf.readByte();
        } else {
            metadataFollows = -1;
        }
        // 2. column_count
        final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuf);
        int actualColumnCount = 0;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0 || metadataFollows == 1) {
            // 3. Field metadata
            for (int readableBytes; actualColumnCount < columnCount; actualColumnCount++) {
                readableBytes = cumulateBuf.readableBytes();
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
            // TODO 抛出特定的异常 在上层捕捉并 关闭连接,这种情况属于协议有误
            throw new JdbdMySQLException("COM_QUERY response packet,not present field metadata.");
        }

        if (actualColumnCount == columnCount && (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            // 4. End of metadata
            if (PacketUtils.hasOnePacket(cumulateBuf)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE); // skip header
                if (EofPacket.isEofPacket(cumulateBuf)) {
                    cumulateBuf.skipBytes(payloadLength);
                } else {
                    // reset reader index
                    // TODO 抛出特定的异常 在上层捕捉并 关闭连接,这种情况属于协议有误
                    cumulateBuf.readerIndex(startReaderIndex);
                    throw new JdbdMySQLException("COM_QUERY response packet,End of metadata isn't EOF packet.");
                }
            }
        }

        // below obtain decodedBuf
        final int endReaderIndex = cumulateBuf.readerIndex();
        // reset reader index
        cumulateBuf.readerIndex(startReaderIndex);
        boolean decodeEnd;
        if (actualColumnCount == columnCount) {
            decodeEnd = true;
            sink.success(cumulateBuf.readRetainedSlice(endReaderIndex - startReaderIndex));
        } else {
            decodeEnd = false;
        }
        return decodeEnd;
    }

    /*################################## blow private method ##################################*/


}
