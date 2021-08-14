package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.WkbType;
import io.jdbd.vendor.type.Geometries;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.FluxSink;

import java.util.function.BiConsumer;

public abstract class PgGeometries {

    protected PgGeometries() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param textValue format: ( x , y )
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.5">Points</a>
     */
    public static Point point(final String textValue) {
        return PgPont.from(textValue);
    }

//    /**
//     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Lines</a>
//     */
//    public static Line lineSegment(String textValue) {
//        return PgLineSegment.from(textValue);
//    }

//    /**
//     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) )
//     * @return {@link LineString} that created by
//     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9">Paths</a>
//     */
//    public static LineString path(final String textValue) {
//        return PgPath.wrap(textValue);
//
//    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-CIRCLE">Circles</a>
     */
    public static Circle circle(String textValue) {
        return PgCircle.from(textValue);
    }


    /**
     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) )
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-POLYGON">Polygons</a>
     */
    private static void polygonToPoints(final String textValue, FluxSink<Point> sink) {

        if (!textValue.startsWith("(") || !textValue.endsWith(")")) {
            sink.error(JdbdExceptions.wrap(createGeometricFormatError(textValue)));
        } else {
            final BiConsumer<Double, Double> pointConsumer = (x, y) -> sink.next(Geometries.point(x, y));
            try {
                // 3. read points
                final int newIndex;
                newIndex = PgGeometries.readPoints(textValue, 1, pointConsumer);
                checkPgGeometricSuffix(textValue, newIndex);
                sink.complete();
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        }

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

    protected static BiConsumer<Double, Double> writePointWkbFunction(final boolean bigEndian, ByteBuf out) {
        return (x, y) -> {
            if (bigEndian) {
                out.writeLong(Double.doubleToLongBits(x));
                out.writeLong(Double.doubleToLongBits(y));
            } else {
                out.writeLongLE(Double.doubleToLongBits(x));
                out.writeLongLE(Double.doubleToLongBits(y));
            }
        };
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
            final BiConsumer<Double, Double> pointConsumer = writePointWkbFunction(bigEndian, out);
            newIndex = readPoints(textValue, 1, pointConsumer);
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
    protected static int readPoints(final String text, final int from
            , final BiConsumer<Double, Double> pointConsumer) {
        final int length = text.length();
        double x, y;
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
            x = Double.parseDouble(text.substring(leftIndex, commaIndex).trim());
            y = Double.parseDouble(text.substring(commaIndex + 1, rightIndex).trim());
            pointConsumer.accept(x, y);

            index = rightIndex + 1;
            if (index >= length) {
                break;
            }
            commaIndex = text.indexOf(',', index);
            leftIndex = text.indexOf('(', index);
            if (commaIndex < 0) {
                if (leftIndex > 0 && leftIndex < text.indexOf(')', index)) {
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
