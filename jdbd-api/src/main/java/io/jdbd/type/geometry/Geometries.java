package io.jdbd.type.geometry;

import io.jdbd.lang.Nullable;

import java.util.Objects;

public abstract class Geometries {

    protected Geometries() {
        throw new UnsupportedOperationException();
    }

    /**
     * WKB-TYPE point,unsigned int.
     */
    public static final int WKB_TYPE_POINT = 1;


    public static Point point(double x, double y) {
        return (x == 0.0D && y == 0.0D) ? DefaultPoint.ZERO : new DefaultPoint(x, y);
    }


    public static Geometry geometryFromWKB(final byte[] wkbBytes, final int offset) throws IllegalArgumentException {
        if (wkbBytes.length < 5) {
            throw createWkbBytesError(wkbBytes.length, 5);
        }
        if (wkbBytes.length - offset < 5) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        final byte byteOrder = wkbBytes[offset];
        final int wkbType;
        if (byteOrder == 0) {
            // big-endian
            wkbType = readIntFromBigEndian(wkbBytes, offset + 1);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = readIntFromLittleEndian(wkbBytes, offset + 1);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        final Geometry geometry;
        switch (wkbType) {
            case WKB_TYPE_POINT:
                geometry = pointFromWKB(wkbBytes, offset);
                break;
            default:
                throw createUnknownWkbTypeError(wkbType);
        }
        return geometry;
    }

    /**
     * <p>
     * Point WKB format:
     *     <ol>
     *         <li>Byte order,1 byte,{@code 0x00}(big-endian) or {@code 0x01}(little-endian)</li>
     *         <li>WKB typ,4 bytes,must be {@link #WKB_TYPE_POINT}</li>
     *         <li>X coordinate,8 bytes,double number</li>
     *         <li>Y coordinate,8 bytes,double number</li>
     *     </ol>
     * </p>
     */
    public static Point pointFromWKB(final byte[] wkbBytes, int offset) throws IllegalArgumentException {
        if (wkbBytes.length < 21) {
            throw createWkbBytesError(wkbBytes.length, 21);
        }
        if (wkbBytes.length - offset < 21) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        final int wkbType;
        final long xBits, yBits;
        final byte byteOrder = wkbBytes[offset++];
        if (byteOrder == 0) {
            // big-endian
            wkbType = readIntFromBigEndian(wkbBytes, offset);
            if (wkbType != WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
            offset += 4;
            xBits = readLongFromBigEndian(wkbBytes, offset);
            offset += 8;
            yBits = readLongFromBigEndian(wkbBytes, offset);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = readIntFromLittleEndian(wkbBytes, offset);
            if (wkbType != WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
            offset += 4;
            xBits = readLongFromLittleEndian(wkbBytes, offset);
            offset += 8;
            yBits = readLongFromLittleEndian(wkbBytes, offset);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        return point(Double.longBitsToDouble(xBits), Double.longBitsToDouble(yBits));
    }

    /**
     * <p>
     * eg: {@code POINT(0.0 0.1)}
     * </p>
     */
    public static Point pointFromWKT(final String wkt) {
        final String startMarker = "POINT(", endMarker = ")";
        if (wkt.startsWith(startMarker) || wkt.endsWith(endMarker)) {
            throw createWktFormatError(null, wkt);
        }
        final int index = wkt.indexOf(" ");
        if (index < 0) {
            throw createWktFormatError(null, wkt);
        }

        try {
            final double x, y;
            x = Double.parseDouble(wkt.substring(startMarker.length(), index));
            y = Double.parseDouble(wkt.substring(index + 1, wkt.length() - 1));
            return point(x, y);
        } catch (NumberFormatException e) {
            throw createWktFormatError(e, wkt);
        }
    }

    /*################################## blow private static method ##################################*/

    private static int readIntFromBigEndian(final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 4) {
            throw createWkbBytesError(wkbBytes.length, 4);
        }
        if (wkbBytes.length - offset < 4) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        return ((wkbBytes[offset++] & 0xFF) << 24)
                | ((wkbBytes[offset++] & 0xFF) << 16)
                | ((wkbBytes[offset++] & 0xFF) << 8)
                | (wkbBytes[offset] & 0xFF);
    }

    private static int readIntFromLittleEndian(final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 4) {
            throw createWkbBytesError(wkbBytes.length, 4);
        }
        if (wkbBytes.length - offset < 4) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        return (wkbBytes[offset++] & 0xFF)
                | ((wkbBytes[offset++] & 0xFF) << 8)
                | ((wkbBytes[offset++] & 0xFF) << 16)
                | ((wkbBytes[offset] & 0xFF) << 24);
    }

    private static long readLongFromBigEndian(final byte[] wkbBytes, final int offset) {
        if (wkbBytes.length < 8) {
            throw createWkbBytesError(wkbBytes.length, 8);
        }
        if (wkbBytes.length - offset < 8) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        long value = 0L;
        final int end = offset + 8;
        for (int i = offset, bitCount = 56; i < end; i++, bitCount -= 8) {
            value |= ((wkbBytes[i] & 0xFFL) << bitCount);
        }
        return value;
    }

    private static long readLongFromLittleEndian(final byte[] wkbBytes, final int offset) {
        if (wkbBytes.length < 8) {
            throw createWkbBytesError(wkbBytes.length, 8);
        }
        if (wkbBytes.length - offset < 8) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        long value = 0;
        final int end = offset + 8;
        for (int i = offset, bitCount = 0; i < end; i++, bitCount += 8) {
            value |= ((wkbBytes[i] & 0xFFL) << bitCount);
        }
        return value;
    }

    private static void longToBigEndianBytes(final long longNum, final byte[] wkbBytes, final int offset) {
        if (wkbBytes.length < 8) {
            throw createWkbBytesError(wkbBytes.length, 8);
        }
        if (wkbBytes.length - offset < 8) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        final int end = offset + 8;
        for (int i = offset, bitCount = 56; i < end; i++, bitCount -= 8) {
            wkbBytes[i] = (byte) (longNum >> bitCount);
        }
    }

    private static void longToLittleEndianBytes(final long longNum, final byte[] wkbBytes, final int offset) {
        if (wkbBytes.length < 8) {
            throw createWkbBytesError(wkbBytes.length, 8);
        }
        if (wkbBytes.length - offset < 8) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        final int end = offset + 8;
        for (int i = offset, bitCount = 0; i < end; i++, bitCount += 8) {
            wkbBytes[i] = (byte) (longNum >> bitCount);
        }
    }

    private static void intToBigEndianBytes(final int intNum, final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 4) {
            throw createWkbBytesError(wkbBytes.length, 4);
        }
        if (wkbBytes.length - offset < 4) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        wkbBytes[offset++] = (byte) (intNum >> 24);
        wkbBytes[offset++] = (byte) (intNum >> 16);
        wkbBytes[offset++] = (byte) (intNum >> 8);
        wkbBytes[offset] = (byte) intNum;
    }

    private static void intToLittleEndianBytes(final int intNum, final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 4) {
            throw createWkbBytesError(wkbBytes.length, 4);
        }
        if (wkbBytes.length - offset < 4) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        wkbBytes[offset++] = (byte) intNum;
        wkbBytes[offset++] = (byte) (intNum >> 8);
        wkbBytes[offset++] = (byte) (intNum >> 16);
        wkbBytes[offset] = (byte) (intNum >> 24);
    }

    private static IllegalArgumentException createOffsetError(final int offset, final int rightBound) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, rightBound));
    }

    private static IllegalArgumentException createWkbBytesError(int wkbBytesLength, int minLength) {
        return new IllegalArgumentException(
                String.format("wkbBytes length[%s] less than min length[%s].", wkbBytesLength, minLength));
    }

    private static IllegalArgumentException createUnknownWkbTypeError(int wkbType) {
        return new IllegalArgumentException(String.format("Unknown WKB-Type[%s]", wkbType));
    }

    private static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byte order[%s]", byteOrder));
    }

    private static IllegalArgumentException createWktFormatError(@Nullable Throwable cause, String wkt) {
        IllegalArgumentException e;
        String message = String.format("WKT format[%s] error.", wkt);
        if (cause == null) {
            e = new IllegalArgumentException(message);
        } else {
            e = new IllegalArgumentException(message, cause);
        }
        return e;
    }


    private static final class DefaultPoint implements Point {

        private static final DefaultPoint ZERO = new DefaultPoint(0.0D, 0.0D);

        private final double x;

        private final double y;

        private DefaultPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.x, this.y);
        }

        @Override
        public boolean equals(Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof Point) {
                Point p = (Point) obj;
                match = p.getX() == this.x && p.getY() == this.y;
            } else {
                match = false;
            }
            return match;
        }

        @Override
        public double getX() {
            return this.x;
        }

        @Override
        public double getY() {
            return this.y;
        }

        @Override
        public byte[] asWKB(final boolean bigEndian) {
            final byte[] wkbBytes = new byte[21];
            int offset = 0;
            if (bigEndian) {
                wkbBytes[offset++] = 0;
                intToBigEndianBytes(WKB_TYPE_POINT, wkbBytes, offset);
                offset += 4;
                longToBigEndianBytes(Double.doubleToLongBits(this.x), wkbBytes, offset);
                offset += 8;
                longToBigEndianBytes(Double.doubleToLongBits(this.y), wkbBytes, offset);
            } else {
                wkbBytes[offset++] = 1;
                intToLittleEndianBytes(WKB_TYPE_POINT, wkbBytes, offset);
                offset += 4;
                longToLittleEndianBytes(Double.doubleToLongBits(this.x), wkbBytes, offset);
                offset += 8;
                longToLittleEndianBytes(Double.doubleToLongBits(this.y), wkbBytes, offset);
            }
            return wkbBytes;
        }

        @Override
        public String toString() {
            return String.format("POINT(%s %s)", this.x, this.y);
        }

    } // DefaultPoint


}
