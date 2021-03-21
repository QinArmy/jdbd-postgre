package io.jdbd.vendor.util;

import io.netty.buffer.ByteBuf;
import org.qinarmy.util.HexUtils;

public abstract class BufferUtils extends HexUtils {

    protected BufferUtils() {
        throw new UnsupportedOperationException();
    }


    public static void writeHexEscapes(final ByteBuf buffer, final byte[] bytes, final int length) {
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            buffer.writeByte(HEX_DIGITS[(b >> 4) & 0xF]); // write highBits
            buffer.writeByte(HEX_DIGITS[b & 0xF]);          // write lowBits
        }

    }


}
