package io.jdbd.vendor.type;

import io.jdbd.type.Point;
import io.jdbd.vendor.util.Geometries;

import java.util.Objects;

final class PointImpl implements Point {

    static PointImpl create(double x, double y) {
        return new PointImpl(x, y);
    }


    private final double x;

    private final double y;

    private PointImpl(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public final double getX() {
        return x;
    }

    @Override
    public final double getY() {
        return y;
    }


    @Override
    public final int hashCode() {
        return Objects.hash(this.x, this.y);
    }

    @Override
    public final boolean equals(Object obj) {
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
    public final String toString() {
        return Geometries.pointToWkt(this);
    }


}
