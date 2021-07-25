package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

abstract class Messages {


    static final Charset CLIENT_CHARSET = StandardCharsets.UTF_8;

    static final byte TERMINATOR = 0;

    static final byte E = 'E';

    static final byte R = 'R';

    static final byte N = 'N';

    static final byte S = 'S';

    static void writeString(ByteBuf packet, String string) {
        packet.writeBytes(string.getBytes(CLIENT_CHARSET));
        packet.writeByte(TERMINATOR);
    }

    static String readString(final ByteBuf message) {
        return new String(readBytesTerm(message), StandardCharsets.UTF_8);
    }

    static byte[] readBytesTerm(final ByteBuf message) {
        final int len;
        len = message.bytesBefore(TERMINATOR);
        if (len < 0) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        final byte[] bytes = new byte[len];
        message.readBytes(bytes);

        if (message.readByte() != 0) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        return bytes;
    }

    static boolean hasOneMessage(ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        return readableBytes > 5
                && readableBytes >= (1 + cumulateBuffer.getInt(cumulateBuffer.readerIndex() + 1));
    }


}
