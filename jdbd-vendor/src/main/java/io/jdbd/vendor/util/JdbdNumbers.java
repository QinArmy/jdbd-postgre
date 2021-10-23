package io.jdbd.vendor.util;

import io.qinarmy.util.NumberUtils;

public abstract class JdbdNumbers extends NumberUtils {

    protected JdbdNumbers() {
        throw new UnsupportedOperationException();
    }


    public static short mapToShort(Object nonNull) {
        short value;
        if (nonNull instanceof Number) {
            value = convertNumberToShort((Number) nonNull);
        } else if (nonNull instanceof String) {
            value = Short.parseShort((String) nonNull);
        } else {
            String m = String.format("Not support java type[%s] to short.", nonNull.getClass().getName());
            throw new IllegalArgumentException(m);
        }
        return value;
    }

    public static byte[] toBinaryBytes(final int value, final boolean bigEndian) {
        return toBinaryBytes(value & 0xFFFF_FFFFL, bigEndian);
    }

    public static byte[] toBinaryBytes(final long value, final boolean bigEndian) {
        long bitSite = (0xFFL << 56);
        int byteLength = 8;
        while ((value & bitSite) == 0) {
            bitSite >>>= 8;
            byteLength--;
            if (byteLength == 1) {
                break;
            }
        }
        final byte[] bytes = new byte[byteLength];
        if (bigEndian) {
            for (int i = 0, bits = (bytes.length - 1) << 3; i < bytes.length; i++, bits -= 8) {
                bytes[i] = (byte) (value >> bits);
            }
        } else {
            for (int i = 0, bits = 0; i < bytes.length; i++, bits += 8) {
                bytes[i] = (byte) (value >> bits);
            }
        }
        return bytes;
    }


}
