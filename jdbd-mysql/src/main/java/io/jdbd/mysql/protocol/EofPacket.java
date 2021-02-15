package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

public final class EofPacket extends TerminatorPacket {

    public static final int EOF_HEADER = 0xFE;

    public static EofPacket read(ByteBuf payloadBuffer, final int capabilities) {
        if (PacketUtils.readInt1(payloadBuffer) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int statusFags, warnings;
        if ((capabilities & ClientCommandProtocol.CLIENT_PROTOCOL_41) != 0) {
            statusFags = PacketUtils.readInt2(payloadBuffer);
            warnings = PacketUtils.readInt2(payloadBuffer);
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        return new EofPacket(statusFags, warnings);
    }


    private EofPacket(int statusFags, int warnings) {
        super(warnings, statusFags);
    }


}
