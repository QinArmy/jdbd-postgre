package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

import java.util.StringJoiner;

public final class RawPacket implements MySQLPacket {

    public static RawPacket readPacket(ByteBuf packetBuf, ServerVersion serverVersion) {
        ByteBuf rawPacket = packetBuf.alloc().buffer(packetBuf.readableBytes());
        packetBuf.readBytes(rawPacket);
        return new RawPacket(rawPacket);
    }

    static boolean isAuthMoreDataPacket(ByteBuf payloadBuf) {
        return PacketUtils.getInt1(payloadBuf, payloadBuf.readerIndex()) == 1;
    }

    private final byte header;

    private final ByteBuf rawPacketBuf;


    private RawPacket(ByteBuf rawPacketBuf) {
        this.header = rawPacketBuf.getByte(rawPacketBuf.readerIndex());
        this.rawPacketBuf = rawPacketBuf.asReadOnly();
    }

    public ByteBuf getRawPacketBuf() {
        return this.rawPacketBuf;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RawPacket.class.getSimpleName() + "[", "]")
                .add("header=" + header)
                .add("readableBytes=" + this.rawPacketBuf.readableBytes())
                .toString();
    }
}
