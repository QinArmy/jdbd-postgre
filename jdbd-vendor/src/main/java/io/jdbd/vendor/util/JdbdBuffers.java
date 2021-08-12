package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import org.qinarmy.util.BufferUtils;

import java.nio.ByteBuffer;

public abstract class JdbdBuffers extends BufferUtils {

    protected JdbdBuffers() {
        throw new UnsupportedOperationException();
    }


    public static void writeUpperCaseHexEscapes(final ByteBuf buffer, final byte[] bytes, final int length) {
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            buffer.writeByte(UPPER_CASE_HEX_DIGITS[(b >> 4) & 0xF]); // write highBits
            buffer.writeByte(UPPER_CASE_HEX_DIGITS[b & 0xF]);          // write lowBits
        }

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
