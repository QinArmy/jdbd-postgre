package io.jdbd.postgre.type;

import io.jdbd.type.Point;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.WkbType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9">Paths</a>
 */
final class PgPath implements LineString {

    /**
     * @param value format: [ ( x1 , y1 ) , ... , ( xn , yn ) ] or ( ( x1 , y1 ) , ... , ( xn , yn ) )
     */
    static PgPath from(String value) {
        value = value.trim();
        if (value.length() < 2) {
            throw new IllegalArgumentException("format error");
        }
        final char first = value.charAt(0), last = value.charAt(value.length() - 1);

        if ((first == '[' && last == ']') || (first == '(' && last == ')')) {
            final List<Point> pointList = new ArrayList<>();
            final int newIndex;
            newIndex = PgGeometries.readPoints(value, 1, pointList::add);
            PgGeometries.checkPgGeometricSuffix(value, newIndex);
            return new PgPath(value, pointList);
        }
        throw new IllegalArgumentException("format error");
    }

    private final String value;

    private final List<Point> pointList;

    private PgPath(String value, List<Point> pointList) {
        this.value = value;
        this.pointList = Collections.unmodifiableList(pointList);
    }


    @Override
    public final Publisher<Point> points() {
        return Flux.fromIterable(pointList());
    }

    @Override
    public final List<Point> pointList() {
        return this.pointList;
    }


    @Override
    public final boolean hasUnderlyingFile() {
        return false;
    }

    @Override
    public final Publisher<byte[]> wkb() {
        return Mono.just(toWkb());
    }

    @Override
    public final Publisher<byte[]> wkt() {
        return Mono.just(toWkt().getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public final byte[] toWkb() {
        final List<Point> pointList = this.pointList;
        final byte[] wkb = new byte[9 + (pointList.size() << 4)];

        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(wkb.length);

        try {
            buffer.writeByte(0);
            buffer.writeInt(WkbType.LINE_STRING.code);
            buffer.writeInt(pointList.size());

            for (Point point : pointList) {
                buffer.writeLong(Double.doubleToLongBits(point.getX()));
                buffer.writeLong(Double.doubleToLongBits(point.getY()));
            }
            buffer.readBytes(wkb);
        } finally {
            buffer.release();
        }
        return wkb;
    }

    @Override
    public final String toWkt() {
        final List<Point> pointList = this.pointList;
        final StringBuilder builder = new StringBuilder(pointList.size() * 12 + 2);
        if (pointList.isEmpty()) {
            builder.append("linestring EMPTY");
        } else {
            builder.append('(');
            int index = 0;
            for (Point point : pointList) {
                if (index > 0) {
                    builder.append(',');
                }
                builder.append(point.getX())
                        .append(' ')
                        .append(point.getY());
                index++;
            }
            builder.append(')');
        }
        return builder.toString();
    }

    @Override
    public final int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof LineString) {
            final LineString v = (LineString) obj;
            if (v.hasUnderlyingFile()) {
                match = false;
            } else {

                match = this.pointList().equals(v.pointList());
            }
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.value;
    }


}
