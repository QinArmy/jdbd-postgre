package io.jdbd.vendor.type;

import io.jdbd.type.Point;
import io.jdbd.type.geometry.Circle;

import java.util.Objects;

final class CircleImpl implements Circle {

    static CircleImpl create(Point center, double radius) {
        return new CircleImpl(center, radius);
    }

    private final Point center;

    private final double radius;

    private CircleImpl(Point center, double radius) {
        this.center = Objects.requireNonNull(center, "center");
        this.radius = radius;
    }

    @Override
    public final Point getCenter() {
        return this.center;
    }

    @Override
    public final double getRadius() {
        return this.radius;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.center, this.radius);
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == null) {
            match = true;
        } else if (obj instanceof Circle) {
            Circle c = (Circle) obj;
            match = this.center.equals(c.getCenter())
                    && Double.compare(this.radius, c.getRadius()) == 0;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return String.format("Circle(center:%s,%s;radius:%s)", this.center.getY(), this.center.getY(), this.radius);
    }


}
