package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

public final class EofPacket implements MySQLPacket {

    public static final int EOF_HEADER = 0xFE;

    public static EofPacket readPacket(ByteBuf packetBuf, final int capabilities) {
        packetBuf.skipBytes(3); //skip payload length
        // skip sequence_id
        short sequenceId = packetBuf.readByte();
        if (PacketUtils.readInt1(packetBuf) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int statusFags, warnings;
        if ((capabilities & ClientCommandProtocol.CLIENT_PROTOCOL_41) != 0) {
            statusFags = PacketUtils.readInt2(packetBuf);
            warnings = PacketUtils.readInt2(packetBuf);
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        return new EofPacket(sequenceId, statusFags, warnings);
    }

    private final short sequenceId;

    private final int statusFags;

    private final int warnings;

    private EofPacket(short sequenceId, int statusFags, int warnings) {
        this.sequenceId = sequenceId;
        this.statusFags = statusFags;
        this.warnings = warnings;
    }

    public short getSequenceId() {
        return this.sequenceId;
    }

    public int getStatusFags() {
        return this.statusFags;
    }

    public int getWarnings() {
        return this.warnings;
    }

    public static boolean isEofPacket(ByteBuf payloadBuf) {
        return PacketUtils.getInt1(payloadBuf, payloadBuf.readerIndex()) == EOF_HEADER;
    }
}
