package io.jdbd.vendor.util;

import io.jdbd.type.Point;

public abstract class JdbdSpatials {


    public static byte[] writePointToWkb(final boolean bigEndian, final Point point) {
        final byte[] wkbArray = new byte[21];

        int offset = 0;
        wkbArray[offset++] = (byte) (bigEndian ? 0 : 1);
        wkbArray[offset++] = 1;

        JdbdNumbers.writeDouble(point.getX(), bigEndian, wkbArray, offset);
        offset += 8;
        JdbdNumbers.writeDouble(point.getY(), bigEndian, wkbArray, offset);
        return wkbArray;
    }

}
