package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.WkbType;
import io.jdbd.vendor.type.Geometries;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Objects;
import java.util.function.Consumer;

public abstract class PgGeometries {

    protected PgGeometries() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Lines</a>
     */
    public static Line lineSegment(String textValue) {
        if (!textValue.startsWith("[") || !textValue.endsWith("]")) {
            throw createGeometricFormatError(textValue);
        }
        final Double[] coordinate = new Double[4];
        final int[] index = new int[]{0};
        Consumer<Double> consumer = d -> {
            if (index[0] < coordinate.length) {
                coordinate[index[0]++] = Objects.requireNonNull(d, "d");
            } else {
                throw createGeometricFormatError(textValue);
            }
        };

        final int newIndex;
        newIndex = PgGeometries.doReadPoints(textValue, 0, consumer);

        if (index[0] < coordinate.length) {
            throw createGeometricFormatError(textValue);
        } else if (newIndex < textValue.length()) {
            for (int i = newIndex, end = textValue.length() - 1; i < end; i++) {
                if (!Character.isWhitespace(textValue.charAt(i))) {
                    throw createGeometricFormatError(textValue);
                }
            }
        }
        return Geometries.line(Geometries.point(coordinate[0], coordinate[1])
                , Geometries.point(coordinate[2], coordinate[3]));
    }

    /**
     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) )
     * @return {@link LineString} that created by
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9">Paths</a>
     */
    public static LineString path(String textValue, final boolean bigEndian) {
        if (!textValue.startsWith("(") || !textValue.endsWith(")")) {
            throw createGeometricFormatError(textValue);
        }
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(1024, 1 << 30);

        //placeholder
        if (bigEndian) {
            out.writeByte(0);
            out.writeInt(WkbType.LINE_STRING.code);
        } else {
            out.writeByte(1);
            out.writeIntLE(WkbType.LINE_STRING.code);
        }
        out.writeZero(4); //placeholder
        Consumer<Double> consumer = d -> {
            if (bigEndian) {
                out.writeLong(Double.doubleToLongBits(d));
            } else {
                out.writeLongLE(Double.doubleToLongBits(d));
            }
        };

        final int newIndex;
        newIndex = PgGeometries.doReadPoints(textValue, 0, consumer);
        checkPgGeometricSuffix(textValue, newIndex);
        return null;
    }


    protected static void checkPgGeometricSuffix(final String textValue, final int from) {
        if (from < textValue.length()) {
            for (int i = from, end = textValue.length() - 1; i < end; i++) {
                if (!Character.isWhitespace(textValue.charAt(i))) {
                    throw createGeometricFormatError(textValue);
                }
            }
        }
    }


    /**
     * <p>
     * Convert postgre line segment to linestring WKB .
     * </p>
     *
     * @param textValue format:[ ( x1 , y1 ) , ( x2 , y2 ) ]
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Line Segments</a>
     */
    protected static byte[] lineSegmentToWkb(final String textValue, final boolean bigEndian) {
        if (!textValue.startsWith("[") || !textValue.endsWith("]")) {
            throw new IllegalArgumentException("Non-postgre line segment ");
        }
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(1024, 1 << 30);

        try {
            if (bigEndian) {
                out.writeByte(0);
                out.writeInt(WkbType.LINE_STRING.code);
                out.writeInt(2);
            } else {
                out.writeByte(1);
                out.writeIntLE(WkbType.LINE_STRING.code);
                out.writeIntLE(2);
            }
            final int newIndex, writerIndex = out.writerIndex();

            final Consumer<Double> consumer = d -> {
                if (bigEndian) {
                    out.writeLong(Double.doubleToLongBits(d));
                } else {
                    out.writeLongLE(Double.doubleToLongBits(d));
                }
            };
            newIndex = doReadPoints(textValue, 1, consumer);
            if ((out.writerIndex() - writerIndex) != 32) {
                throw createGeometricFormatError(textValue);
            }
            for (int i = newIndex, end = textValue.length() - 1; i < end; i++) {
                if (!Character.isWhitespace(textValue.charAt(i))) {
                    throw createGeometricFormatError(textValue);
                }
            }
            byte[] bytes = new byte[out.readableBytes()];

            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    /**
     * @return new index of text
     */
    protected static int doReadPoints(final String text, final int from
            , final Consumer<Double> consumer) {
        final int length = text.length();
        double d;
        int index = from;
        for (int leftIndex, commaIndex, rightIndex; index < length; ) {
            if (Character.isWhitespace(text.charAt(index))) {
                index++;
                continue;
            }
            leftIndex = text.indexOf('(', index);
            rightIndex = text.indexOf(')', index);
            if (leftIndex < 0) {
                // no more point
                break;
            } else if (rightIndex < 0 || leftIndex > rightIndex) {
                throw createGeometricFormatError(text);
            }
            leftIndex++;
            commaIndex = text.indexOf(',', leftIndex);
            if (commaIndex < 0 || commaIndex > rightIndex) {
                throw createGeometricFormatError(text);
            }
            d = Double.parseDouble(text.substring(leftIndex, commaIndex).trim());
            consumer.accept(d);
            d = Double.parseDouble(text.substring(commaIndex + 1, rightIndex).trim());
            consumer.accept(d);

            index = rightIndex + 1;
            if (index >= length) {
                break;
            }
            commaIndex = text.indexOf(',', index);
            leftIndex = text.indexOf('(', index);
            if (commaIndex < 0) {
                if (text.indexOf('(', index) < text.indexOf(')', index)) {
                    throw createGeometricFormatError(text);
                }
                // no more point
                break;
            } else if (leftIndex < 0 || leftIndex < commaIndex) {
                throw createGeometricFormatError(text);
            } else {
                index = leftIndex;
            }

        }
        return index;
    }


    protected static IllegalArgumentException createGeometricFormatError(String textValue) {
        return new IllegalArgumentException(String.format("Geometric[%s] format error.", textValue));
    }


}
