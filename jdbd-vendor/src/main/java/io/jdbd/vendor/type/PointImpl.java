package io.jdbd.vendor.type;

import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.GeometryUtils;

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
        return this.x;
    }

    @Override
    public final double getY() {
        return this.y;
    }


    @Override
    public final byte[] toWkb() {
        return GeometryUtils.pointToWkb(this, false);
    }

    @Override
    public final String toWkt() {
        return GeometryUtils.pointToWkt(this);
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
        return GeometryUtils.pointToWkt(this);
    }


}
