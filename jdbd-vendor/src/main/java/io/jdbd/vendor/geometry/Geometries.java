package io.jdbd.vendor.geometry;

import io.jdbd.lang.Nullable;
import io.jdbd.type.geometry.*;
import io.jdbd.vendor.util.JdbdNumberUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class Geometries {

    protected Geometries() {
        throw new UnsupportedOperationException();
    }

    public static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;


    public static Point point(double x, double y) {
        return (x == 0.0D && y == 0.0D) ? DefaultPoint.ZERO : new DefaultPoint(x, y);
    }

    public static LineString line(Point one, Point two) {
        return DefaultSmallLineString.line(one, two);
    }

    public static SmallLineString lineString(List<Point> pointList, final boolean assemble) {
        final List<Point> list;
        if (assemble) {
            list = new ArrayList<>(pointList.size());
            list.addAll(pointList);
        } else {
            list = pointList;
        }
        return DefaultSmallLineString.create(list);
    }


    public static SmallGeometry geometryFromWkb(final byte[] wkbBytes, final int offset)
            throws IllegalArgumentException {
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
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset + 1);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset + 1, 4);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        final SmallGeometry geometry;
        switch (wkbType) {
            case Point.WKB_TYPE_POINT:
                geometry = pointFromWkb(wkbBytes, offset);
                break;
            case LineString.WKB_TYPE_LINE_STRING:
                geometry = lineStringFromWkb(wkbBytes, offset);
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
     *         <li>WKB typ,4 bytes,must be {@link Point#WKB_TYPE_POINT}</li>
     *         <li>X coordinate,8 bytes,double number</li>
     *         <li>Y coordinate,8 bytes,double number</li>
     *     </ol>
     * </p>
     */
    public static Point pointFromWkb(final byte[] wkbBytes, int offset) throws IllegalArgumentException {
        if (wkbBytes.length < Point.WKB_BYTES) {
            throw createWkbBytesError(wkbBytes.length, Point.WKB_BYTES);
        }
        if (wkbBytes.length - offset < Point.WKB_BYTES) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        final int wkbType;
        final long xBits, yBits;
        final byte byteOrder = wkbBytes[offset++];
        if (byteOrder == 0) {
            // big-endian
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
            if (wkbType != Point.WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
            offset += 4;
            xBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, offset, 8);
            offset += 8;
            yBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, offset, 8);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
            if (wkbType != Point.WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
            offset += 4;
            xBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, offset, 8);
            offset += 8;
            yBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, offset, 8);
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
    public static Point pointFromWkt(final String wkt) throws IllegalArgumentException {
        final String startMarker = "POINT(", endMarker = ")";
        if (!wkt.startsWith(startMarker) || !wkt.endsWith(endMarker)) {
            throw createWktFormatError(null, wkt);
        }
        final int index = wkt.indexOf(" ", startMarker.length());
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

    public static SmallLineString lineStringFromWkb(final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 9) {
            throw createWkbBytesError(wkbBytes.length, 9);
        }
        if (wkbBytes.length - offset < 9) {
            throw createOffsetError(offset, wkbBytes.length);
        }
        // 1. below parse byteOrder,wkbType,pointSize
        final int wkbType, pointSize;
        final byte byteOrder = wkbBytes[offset++];
        if (byteOrder == 0) {
            // big-endian
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
            if (wkbType != LineString.WKB_TYPE_LINE_STRING) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't LINESTRING[%s]"
                        , wkbType, LineString.WKB_TYPE_LINE_STRING));
            }
            offset += 4;
            pointSize = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
            if (wkbType != LineString.WKB_TYPE_LINE_STRING) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't LINESTRING[%s]"
                        , wkbType, LineString.WKB_TYPE_LINE_STRING));
            }
            offset += 4;
            pointSize = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        offset += 4;
        if (pointSize < 2 || pointSize > SmallLineString.MAX_POINT_LiST_SIZE) {
            throw createSmallLineSizeError(pointSize);
        }

        // 2. below parse point list.
        final int end = offset + (pointSize * Point.WKB_BYTES);
        if (wkbBytes.length < end) {
            throw createWkbBytesError(wkbBytes.length, end);
        }
        List<Point> pointList = new ArrayList<>(pointSize);
        for (int i = 0; i < pointSize; i++) {
            pointList.add(pointFromWkb(wkbBytes, offset));
            offset += Point.WKB_BYTES;
        }
        return lineString(pointList, false);
    }


    public static SmallLineString lineStringFromWkt(final String wkt) {
        final String startMarker = "LINESTRING(", endMarker = ")";
        if (!wkt.startsWith(startMarker) || !wkt.endsWith(endMarker)) {
            throw createWktFormatError(null, wkt);
        }

        String pointsSegment = wkt.substring(startMarker.length(), wkt.length() - 1);
        final String[] pairArray = pointsSegment.split(",");
        if (pairArray.length < 2 || pairArray.length > SmallLineString.MAX_POINT_LiST_SIZE) {
            throw createSmallLineSizeError(pairArray.length);
        }

        try {
            List<Point> pointList = new ArrayList<>(pairArray.length);
            String pair;
            double x, y;
            for (int i = 0, spaceIndex; i < pairArray.length; i++) {
                pair = pairArray[i];
                spaceIndex = pair.indexOf(' ');
                if (spaceIndex < 0) {
                    throw createWktFormatError(null, wkt);
                }
                x = Double.parseDouble(pair.substring(0, spaceIndex));
                y = Double.parseDouble(pair.substring(spaceIndex + 1));
                pointList.add(point(x, y));
            }
            return lineString(pointList, false);
        } catch (NumberFormatException e) {
            throw createWktFormatError(e, wkt);
        }
    }

    /*################################## blow packet static method ##################################*/


    static void pointAsWkb(final Point point, final boolean bigEndian, final byte[] wkbBytes, int offset)
            throws IllegalArgumentException {
        if (wkbBytes.length < Point.WKB_BYTES) {
            throw createWkbBytesError(wkbBytes.length, Point.WKB_BYTES);
        }
        if (wkbBytes.length - offset < Point.WKB_BYTES) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        if (bigEndian) {
            wkbBytes[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(Point.WKB_TYPE_POINT, wkbBytes, offset);
            offset += 4;
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(point.getX()), wkbBytes, offset);
            offset += 8;
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(point.getY()), wkbBytes, offset);
        } else {
            wkbBytes[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(Point.WKB_TYPE_POINT, wkbBytes, offset, 4);
            offset += 4;
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(point.getX()), wkbBytes, offset, 8);
            offset += 8;
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(point.getY()), wkbBytes, offset, 8);
        }

    }

    static StringBuilder pointAsWkt(Point point, StringBuilder builder) {
        return builder.append("POINT(")
                .append(point.getX())
                .append(" ")
                .append(point.getY())
                .append(")");
    }

    static StringBuilder lineStringAsWkt(final SmallLineString lineString, final StringBuilder builder) {
        final List<Point> pointList = lineString.pointList();
        final int size = pointList.size();
        builder.append("LINESTRING(");
        Point point;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(",");
            }
            point = pointList.get(i);
            builder.append(point.getX())
                    .append(" ")
                    .append(point.getY());
        }
        return builder.append(")");
    }

    static void lineStringAsWkbBytes(final List<Point> pointList, final boolean bigEndian, final byte[] wkbBytes
            , int offset) throws IllegalArgumentException {
        final int size = pointList.size();
        if (size < 2) {
            throw createSmallLineSizeError(size);
        }
        final int wkbTotalLength = 9 + (size * Point.WKB_BYTES);
        if (wkbBytes.length < wkbTotalLength) {
            throw createWkbBytesError(wkbBytes.length, wkbTotalLength);
        }
        if (wkbBytes.length - offset < wkbTotalLength) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        if (bigEndian) {
            wkbBytes[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(LineString.WKB_TYPE_LINE_STRING, wkbBytes, offset, 4);
            offset += 4;
            JdbdNumberUtils.intToBigEndian(size, wkbBytes, offset, 4);
        } else {
            wkbBytes[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(LineString.WKB_TYPE_LINE_STRING, wkbBytes, offset, 4);
            offset += 4;
            JdbdNumberUtils.intToLittleEndian(size, wkbBytes, offset, 4);
        }
        offset += 4;

        for (int i = 0; i < size; i++) {
            pointAsWkb(pointList.get(i), bigEndian, wkbBytes, offset);
            offset += Point.WKB_BYTES;
        }

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

    private static IllegalArgumentException createSmallLineSizeError(int pointSize) {
        return new IllegalArgumentException(String.format(
                "%s point size[%s] must in [2,%s],please use %s ."
                , SmallLineString.class.getSimpleName()
                , pointSize
                , SmallLineString.MAX_POINT_LiST_SIZE
                , LargeLineString.class.getName()));
    }


}
