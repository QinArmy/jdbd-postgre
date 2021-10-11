package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;

public final class EofPacket extends TerminatorPacket {

    public static final int EOF_HEADER = 0xFE;

    public static EofPacket read(ByteBuf payloadBuffer, final int capabilities) {
        if (Packets.readInt1AsInt(payloadBuffer) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int statusFags, warnings;
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            statusFags = Packets.readInt2AsInt(payloadBuffer);
            warnings = Packets.readInt2AsInt(payloadBuffer);
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        return new EofPacket(statusFags, warnings);
    }


    private EofPacket(int statusFags, int warnings) {
        super(warnings, statusFags);
    }


}
