package io.jdbd.postgre.type;

import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.WkbType;
import io.jdbd.vendor.type.Geometries;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;

import java.nio.channels.FileChannel;
import java.util.function.BiConsumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9">Paths</a>
 */
final class PgPath implements LineString {

    /**
     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) ) or ( ( x1 , y1 ) , ... , ( xn , yn ) )
     */
    static PgPath wrap(String textValue) {
        if (!(textValue.startsWith("(") && textValue.endsWith(")"))
                && !(textValue.startsWith("[") && textValue.endsWith("]"))) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        return new PgPath(textValue);
    }


    private final String textValue;

    private PgPath(String textValue) {
        this.textValue = textValue;
    }

    @Override
    public final boolean isArray() {
        return true;
    }

    @Override
    public final byte[] toWkb() throws IllegalStateException {
        // assume 5 byte each number, initialCapacity = WKB header(9) + point count * 16 .
        // one pont char count = 5 * 2 + '(' + ',' + ')' + ',' = 14
        // so below
        final int initialCapacity = 9 + (((textValue.length() - 2) / 14) << 4);
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(initialCapacity, Integer.MAX_VALUE);

        // 1. write wkb header
        out.writeByte(0);
        out.writeInt(WkbType.LINE_STRING.code);
        out.writeZero(4); //placeholder

        // 2. create consumer function
        final BiConsumer<Double, Double> pointConsumer = (x, y) -> {
            out.writeLong(Double.doubleToLongBits(x));
            out.writeLong(Double.doubleToLongBits(y));
        };

        try {
            // 3. read points
            final int newIndex;
            newIndex = PgGeometries.readPoints(textValue, 1, pointConsumer);
            PgGeometries.checkPgGeometricSuffix(textValue, newIndex);
            // 4. validate point count
            final int pointCount = (out.readableBytes() - 9) >> 4;
            if (pointCount < 2) {
                throw new IllegalStateException(String.format("textValue[%s] isn't postgre path.", textValue));
            }
            // 5 write point count.
            final int writerIndex = out.writerIndex();
            out.writerIndex(5); // index of placeholder
            out.writeInt(pointCount);
            out.writerIndex(writerIndex);

            // 6. copy wkb as array
            byte[] wkbBytes = new byte[out.readableBytes()];
            out.readBytes(wkbBytes);
            return wkbBytes;
        } finally {
            out.release();
        }
    }

    @Override
    public final String toWkt() {
        final String tag = "LINESTRING";
        final StringBuilder builder = new StringBuilder(tag)
                .append(" ");
        final int[] count = new int[]{0};
        final BiConsumer<Double, Double> pointConsumer = (x, y) -> {
            if (count[0] > 0) {
                builder.append(",");
            }
            builder.append(x)
                    .append(" ")
                    .append(y);
            count[0]++;
        };

        final int newIndex;
        newIndex = PgGeometries.readPoints(this.textValue, 1, pointConsumer);
        PgGeometries.checkPgGeometricSuffix(textValue, newIndex);

        if (count[0] == 0) {
            builder.append("EMPTY");
        } else {
            builder.setCharAt(tag.length(), '(');
            builder.append(")");
        }
        return builder.toString();
    }

    @Override
    public final Flux<Point> points() {
        return Flux.create(sink -> {
            final BiConsumer<Double, Double> pointConsumer = (x, y) -> sink.next(Geometries.point(x, y));
            try {
                final int newIndex;
                newIndex = PgGeometries.readPoints(this.textValue, 1, pointConsumer);
                PgGeometries.checkPgGeometricSuffix(textValue, newIndex);
                sink.complete();
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    @Override
    public final byte[] asArray() throws IllegalStateException {
        return toWkb();
    }

    @Override
    public final FileChannel openReadOnlyChannel() throws IllegalStateException {
        throw new IllegalStateException("Non-underlying file,use asArray() method.");
    }

    @Override
    public final String toString() {
        return this.textValue;
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return super.equals(obj);
    }


}
