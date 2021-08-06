package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import org.qinarmy.util.BufferUtils;

public abstract class JdbdBufferUtils extends BufferUtils {

    protected JdbdBufferUtils() {
        throw new UnsupportedOperationException();
    }


    public static void writeUpperCaseHexEscapes(final ByteBuf buffer, final byte[] bytes, final int length) {
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            buffer.writeByte(UPPER_CASE_HEX_DIGITS[(b >> 4) & 0xF]); // write highBits
            buffer.writeByte(UPPER_CASE_HEX_DIGITS[b & 0xF]);          // write lowBits
        }

    }


}
