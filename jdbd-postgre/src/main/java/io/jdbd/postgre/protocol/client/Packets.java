package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

abstract class Packets {


    static final Charset CLIENT_CHARSET = StandardCharsets.UTF_8;

    static final byte TERMINATOR = 0;

    static final byte ERROR = 'E';

    static void writeString(ByteBuf packet, String string) {
        packet.writeBytes(string.getBytes(CLIENT_CHARSET));
        packet.writeByte(TERMINATOR);
    }

    static String readString(final ByteBuf packet) {
        return new String(readBytesTerm(packet), StandardCharsets.UTF_8);
    }

    static byte[] readBytesTerm(final ByteBuf packet) {
        final int len;
        len = packet.bytesBefore(TERMINATOR);
        if (len < 0) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        final byte[] bytes = new byte[len];
        packet.readBytes(bytes);

        if (packet.readByte() != 0) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        return bytes;
    }

    static boolean hasOnePacket(ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        return readableBytes > 5
                && readableBytes >= (1 + cumulateBuffer.getInt(cumulateBuffer.readerIndex() + 1));
    }


}
