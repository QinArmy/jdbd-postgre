package io.jdbd.postgre.type;

import io.jdbd.type.LongBinary;
import io.jdbd.vendor.util.WkbType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.function.Consumer;

public abstract class PgGeometries {

    protected PgGeometries() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Lines</a>
     */
    public static LongBinary lineSegment(String textValue, boolean bigEndian) {
        return PgLineSegment.parse(textValue, bigEndian);
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


    private static IllegalArgumentException createGeometricFormatError(String textValue) {
        return new IllegalArgumentException(String.format("Geometric[%s] format error.", textValue));
    }


}
