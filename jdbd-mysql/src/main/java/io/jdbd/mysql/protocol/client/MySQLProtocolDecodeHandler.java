package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import reactor.netty.Connection;

import java.util.List;

final class MySQLProtocolDecodeHandler extends ByteToMessageDecoder {

    static void addMySQLDecodeHandler(Connection connection) {
        connection.addHandlerFirst(MySQLProtocolDecodeHandler.class.getSimpleName()
                , new MySQLProtocolDecodeHandler());
    }

    private MySQLProtocolDecodeHandler() {

    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf byteBuf, List<Object> list) {
        int payloadLength = PacketUtils.getInt3(byteBuf, byteBuf.readerIndex());
        int packetTotalLen = PacketUtils.HEADER_SIZE + payloadLength;
        int readableBytes = byteBuf.readableBytes();

        if (readableBytes < packetTotalLen) {
            return;
        }
        if (payloadLength == ClientProtocol.MAX_PACKET_SIZE) {
            int payloadLengthSum = obtainMultiPacketLength(byteBuf);
            if (payloadLengthSum > 0) {
                list.add(decodeMultiPacket(byteBuf, payloadLengthSum));
            }
        } else {
            list.add(byteBuf.readRetainedSlice(packetTotalLen));
        }
    }

    private ByteBuf decodeMultiPacket(ByteBuf byteBuf, final int payloadLengthSum) {
        final ByteBuf payloadBuf = byteBuf.alloc().buffer(payloadLengthSum);

        for (int i = 0, payloadLength, sequence; ; i++) {
            payloadLength = PacketUtils.readInt3(byteBuf);
            sequence = PacketUtils.readInt1(byteBuf);
            if (sequence != i) {
                payloadBuf.release();
                throw new JdbdMySQLException("sequence should be [{}] but [{}].", i, sequence);
            }
            byteBuf.readBytes(payloadBuf, payloadLength);
            if (payloadLength != ClientProtocol.MAX_PACKET_SIZE) {
                break;
            }
        }
        return payloadBuf.asReadOnly();
    }

    private int obtainMultiPacketLength(ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();

        int readIndex = byteBuf.readerIndex();
        int payloadLength = PacketUtils.getInt3(byteBuf, readIndex);
        int packetLengthSum = PacketUtils.HEADER_SIZE + payloadLength;
        int payloadLengthSum = payloadLength;

        for (int sequence = 0; payloadLength == ClientProtocol.MAX_PACKET_SIZE; sequence++) {

            if (PacketUtils.getInt1(byteBuf, readIndex + 3) != sequence) {
                throw new JdbdMySQLException("sequence should be [{}] but [{}]."
                        , sequence, PacketUtils.getInt1(byteBuf, readIndex + 3));
            }
            if (readableBytes < packetLengthSum + PacketUtils.HEADER_SIZE) {
                payloadLengthSum = -1;
                break;
            }

            readIndex = packetLengthSum;
            payloadLength = PacketUtils.getInt3(byteBuf, readIndex);
            packetLengthSum += (PacketUtils.HEADER_SIZE + payloadLength);
            payloadLengthSum += payloadLength;

            if (readableBytes < packetLengthSum) {
                payloadLengthSum = -1;
                break;
            }

        }
        return payloadLengthSum;
    }


}
