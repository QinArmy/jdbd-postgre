package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

public final class EofPacket implements MySQLPacket {

    public static final int EOF_HEADER = 0xFE;

    public static EofPacket readPacket(ByteBuf packetBuf, final int serverCapabilities) {
        int payloadLength = PacketUtils.readInt3(packetBuf);
        // skip sequence_id
        packetBuf.readByte();
        if (PacketUtils.readInt1(packetBuf) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int statusFags, warnings;
        if ((serverCapabilities & ClientCommandProtocol.CLIENT_PROTOCOL_41) != 0) {
            statusFags = PacketUtils.readInt2(packetBuf);
            warnings = PacketUtils.readInt2(packetBuf);
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        return new EofPacket(statusFags, warnings);
    }


    private final int statusFags;

    private final int warnings;

    private EofPacket(int statusFags, int warnings) {
        this.statusFags = statusFags;
        this.warnings = warnings;
    }

    public int getStatusFags() {
        return this.statusFags;
    }

    public int getWarnings() {
        return this.warnings;
    }

    public static boolean isEofPacket(ByteBuf packetBuf) {
        return PacketUtils.getInt1(packetBuf, PacketUtils.HEADER_SIZE) == EOF_HEADER
                && PacketUtils.getInt3(packetBuf, packetBuf.readerIndex()) < 5;
    }
}
