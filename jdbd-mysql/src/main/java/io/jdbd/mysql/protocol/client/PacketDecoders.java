package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

public abstract class PacketDecoders {


    protected PacketDecoders() {
        throw new UnsupportedOperationException();
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
    public static ByteBuf comQueryResponseMetaDecoder(final ByteBuf cumulateBuf, final int negotiatedCapability) {
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
