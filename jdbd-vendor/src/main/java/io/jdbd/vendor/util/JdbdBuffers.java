package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import io.qinarmy.util.BufferUtils;

import java.nio.ByteBuffer;

public abstract class JdbdBuffers extends BufferUtils {

    protected JdbdBuffers() {
        throw new UnsupportedOperationException();
    }


    @Deprecated
    public static void writeUpperCaseHexEscapes(final ByteBuf message, final byte[] bytes, final int length) {
        message.writeBytes(hexEscapes(true, bytes, length));
    }


    public static void cumulateBuffer(final ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            buffer.clear();
            return;
        }
        final int remaining = buffer.remaining();
        if (buffer.hasArray()) {
            final byte[] array = buffer.array();
            for (int index = 0, i = buffer.position(), limit = buffer.limit(); i < limit; i++, index++) {
                array[index] = array[i];
            }
        } else {
            for (int index = 0, i = buffer.position(), limit = buffer.limit(); i < limit; i++, index++) {
                buffer.put(index, buffer.get(i));
            }
        }
        buffer.position(0)
                .limit(remaining);
    }


}
