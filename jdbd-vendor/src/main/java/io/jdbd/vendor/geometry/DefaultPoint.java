package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Point;

import java.util.Objects;

final class DefaultPoint implements Point {


    static final DefaultPoint ZERO = new DefaultPoint(0.0D, 0.0D);

    private final double x;

    private final double y;

    private final int textLength;

    DefaultPoint(double x, double y) {
        this.x = x;
        this.y = y;
        this.textLength = Double.toString(x).length() + Double.toString(y).length() + 1;
    }

    @Override
    public String asWkt() {
        return Geometries.pointAsWkt(this, new StringBuilder())
                .toString();
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.x, this.y);
    }

    @Override
    public boolean equals(Object obj) {
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
    public double getX() {
        return this.x;
    }

    @Override
    public double getY() {
        return this.y;
    }


    @Override
    public int getTextLength() {
        return this.textLength;
    }

    @Override
    public byte[] asWkbBytes(final boolean bigEndian) {
        final byte[] wkbBytes = new byte[WKB_BYTES];
        Geometries.pointAsWkb(this, bigEndian, wkbBytes, 0);
        return wkbBytes;
    }

    @Override
    public String toString() {
        return asWkt();
    }

} // DefaultPoint
