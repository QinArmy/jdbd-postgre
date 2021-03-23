package io.jdbd.vendor.util;

public abstract class GeometryConvertUtils {

    protected GeometryConvertUtils() {
        throw new UnsupportedOperationException();
    }

    public static final byte WKB_POINT = 1;

    public static final byte WKB_POINT_BYTES = 21;


    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        String startMarker = "POINT(", endMarker = ")";
        if (!pointWkt.startsWith(startMarker) || !pointWkt.endsWith(endMarker)) {
            throw createWktFormatError("POINT");
        }
        String[] coordinateArray = pointWkt.substring(startMarker.length(), pointWkt.length() - 1).split(" ");
        if (coordinateArray.length != 2) {
            throw createWktFormatError("POINT");
        }
        double x, y;
        x = Double.parseDouble(coordinateArray[0]);
        y = Double.parseDouble(coordinateArray[1]);

        byte[] wkbArray = new byte[WKB_POINT_BYTES];
        int offset = 0;
        if (bigEndian) {
            wkbArray[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(WKB_POINT, wkbArray, offset, 4);
            offset += 4;
            JdbdNumberUtils.doubleToEndian(true, x, wkbArray, offset);
            offset += 8;
            JdbdNumberUtils.doubleToEndian(true, y, wkbArray, offset);
        } else {
            wkbArray[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(WKB_POINT, wkbArray, offset, 4);
            offset += 4;
            JdbdNumberUtils.doubleToEndian(false, x, wkbArray, offset);
            offset += 8;
            JdbdNumberUtils.doubleToEndian(false, y, wkbArray, offset);
        }
        return wkbArray;
    }


    public static void pointWkbReverse(final byte[] wkbArray) {
        if (wkbArray.length != WKB_POINT_BYTES) {
            throw createWkbLengthError("POINT", wkbArray.length, WKB_POINT_BYTES);
        }

        checkByteOrder(wkbArray[0]);

        wkbArray[0] ^= 1;
        int offset = 1;
        JdbdArrayUtils.reverse(wkbArray, offset, 4, 1);
        offset += 4;
        JdbdArrayUtils.reverse(wkbArray, offset, 8, 2);

    }

    public static String pointToWkt(final byte[] wkbArray) {
        if (wkbArray.length != WKB_POINT_BYTES) {
            throw createWkbLengthError("POINT", wkbArray.length, WKB_POINT_BYTES);
        }
        final byte byteOrder = checkByteOrder(wkbArray[0]);
        int offset = 1;
        final int wkbType;
        final double x, y;
        if (byteOrder == 0) {
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
            if (wkbType != WKB_POINT) {
                throw createWkbTypeNotMatchError("POINT", wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
        } else {
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
            if (wkbType != WKB_POINT) {
                throw createWkbTypeNotMatchError("POINT", wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
        }
        return String.format("POINT(%s %s)", x, y);
    }




    /*################################## blow private method ##################################*/

    static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }


    private static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }


    private static IllegalArgumentException createWkbLengthError(String type, int length, int exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }


    private static IllegalArgumentException createWktFormatError(String wktType) {
        return new IllegalArgumentException(String.format("Not %s WKT format.", wktType));
    }

    private static IllegalArgumentException createWkbTypeNotMatchError(String type, int wkbType) {
        return new IllegalArgumentException(String.format("WKB type[%s] and %s not match.", wkbType, type));
    }

}
