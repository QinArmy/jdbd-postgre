package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

public final class RawPacket implements MySQLPacket {

    public static RawPacket readPacket(ByteBuf packetBuf, ServerVersion serverVersion) {
        int readIndex;
        if (!serverVersion.meetsMinimum(5, 5, 16)) {
            readIndex = PacketUtils.HEADER_SIZE - 1;
        } else {
            readIndex = PacketUtils.HEADER_SIZE;
        }
        packetBuf.readerIndex(readIndex);
        ByteBuf rawPacket = packetBuf.alloc().buffer(packetBuf.readableBytes());
        packetBuf.readBytes(rawPacket);
        return new RawPacket(rawPacket);
    }

    private final ByteBuf rawPacketBuf;

    private RawPacket(ByteBuf rawPacketBuf) {
        this.rawPacketBuf = rawPacketBuf;
    }

    public ByteBuf getRawPacketBuf() {
        return this.rawPacketBuf;
    }
}
