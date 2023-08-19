package io.jdbd.postgre.type;

import io.jdbd.type.Point;
import io.jdbd.type.geo.Line;
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
import java.util.Objects;
import java.util.function.Consumer;

final class PgLineSegment implements Line {

    /**
     * @param value format:[ ( x1 , y1 ) , ( x2 , y2 ) ]
     */
    static PgLineSegment from(String value) {
        value = value.trim();
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            throw new IllegalArgumentException("format error");
        }
        final Point[] pointArray = new Point[2];
        final Consumer<Point> consumer = point -> {
            boolean success = false;
            for (int i = 0; i < pointArray.length; i++) {
                if (pointArray[i] == null) {
                    pointArray[i] = Objects.requireNonNull(point, "point");
                    success = true;
                    break;
                }
            }
            if (!success) {
                throw new IllegalArgumentException("format error,too many point.");
            }
        };
        final int newIndex;
        newIndex = PgGeometries.readPoints(value, 1, consumer);
        PgGeometries.checkPgGeometricSuffix(value, newIndex);
        return new PgLineSegment(value, pointArray[0], pointArray[1]);
    }

    private final String value;

    private final Point point1;

    private final Point point2;

    private PgLineSegment(String value, Point point1, Point point2) {
        this.value = value;
        this.point1 = point1;
        this.point2 = point2;
    }

    @Override
    public final Point getPoint1() {
        return this.point1;
    }

    @Override
    public final Point getPoint2() {
        return this.point2;
    }

    @Override
    public final Publisher<Point> points() {
        return Flux.just(this.point1, this.point2);
    }

    @Override
    public final List<Point> pointList() {
        final List<Point> list = new ArrayList<>(2);
        list.add(this.point1);
        list.add(this.point2);
        return Collections.unmodifiableList(list);
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
        final byte[] wkb = new byte[41];
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(wkb.length);

        buffer.writeByte(0);
        buffer.writeInt(WkbType.LINE_STRING.code);
        buffer.writeInt(2);

        buffer.writeLong(Double.doubleToLongBits(this.point1.getX()));
        buffer.writeLong(Double.doubleToLongBits(this.point1.getY()));
        buffer.writeLong(Double.doubleToLongBits(this.point2.getX()));
        buffer.writeLong(Double.doubleToLongBits(this.point2.getY()));

        buffer.readBytes(wkb);

        buffer.release();
        return wkb;
    }

    @Override
    public final String toWkt() {
        return String.format("linestring(%s %s,%s %s)"
                , this.point1.getX(), this.point1.getY()
                , this.point2.getX(), this.point2.getY()
        );
    }


    @Override
    public final int hashCode() {
        return Objects.hash(this.point1, this.point2);
    }

    @Override
    public final boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Line) {
            final Line v = (Line) obj;
            match = this.point1.equals(v.getPoint1())
                    && this.point2.equals(v.getPoint2());
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
