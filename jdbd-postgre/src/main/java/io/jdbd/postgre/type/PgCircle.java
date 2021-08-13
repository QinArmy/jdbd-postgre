package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.Geometries;

import java.util.Objects;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-CIRCLE">Circles</a>
 */
final class PgCircle implements Circle {

    /**
     * @param textValue format : < ( x , y ) , r >
     */
    static PgCircle from(final String textValue) {
        if (!textValue.startsWith("<") || !textValue.endsWith(">")) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        int leftIndex, rightIndex, commaIndex;
        leftIndex = textValue.indexOf('(', 1);
        rightIndex = textValue.indexOf(')', 1);
        commaIndex = textValue.indexOf(',', 1);
        if (leftIndex > 0 && commaIndex > leftIndex && rightIndex > commaIndex) {
            double x, y, r;
            x = Double.parseDouble(textValue.substring(leftIndex + 1, commaIndex).trim());
            y = Double.parseDouble(textValue.substring(commaIndex + 1, rightIndex).trim());
            commaIndex = textValue.indexOf(',', rightIndex + 1);
            if (commaIndex < 0) {
                throw PgGeometries.createGeometricFormatError(textValue);
            }
            r = Double.parseDouble(textValue.substring(commaIndex + 1, textValue.length() - 1).trim());
            return new PgCircle(textValue, Geometries.point(x, y), r);
        } else {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
    }

    private final String textValue;

    private final Point point;

    private final double radius;

    private PgCircle(String textValue, Point point, double radius) {
        this.textValue = textValue;
        this.point = point;
        this.radius = radius;
    }

    @Override
    public final Point getCenter() {
        return this.point;
    }

    @Override
    public final double getRadius() {
        return this.radius;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.point, radius);
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Circle) {
            Circle c = (Circle) obj;
            match = this.point.equals(c.getCenter()) && this.radius == c.getRadius();
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.textValue;
    }


}
