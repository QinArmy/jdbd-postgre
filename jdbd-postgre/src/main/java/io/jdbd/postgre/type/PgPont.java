package io.jdbd.postgre.type;

import io.jdbd.type.Point;

import java.util.Objects;

final class PgPont implements Point {

    /**
     * @param textValue format: ( x , y )
     * @throws IllegalArgumentException when textValue error.
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.5">Points</a>
     */
    static PgPont from(final String textValue) {
        if (!textValue.startsWith("(") || !textValue.endsWith(")")) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        final int commaIndex;
        commaIndex = textValue.indexOf(',', 1);
        if (commaIndex < 0) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        final double x, y;
        x = Double.parseDouble(textValue.substring(1, commaIndex).trim());
        y = Double.parseDouble(textValue.substring(commaIndex + 1, textValue.length() - 1).trim());
        return new PgPont(textValue, x, y);
    }

    private final String textValue;

    private final double x;

    private final double y;

    private PgPont(String textValue, double x, double y) {
        this.textValue = textValue;
        this.x = x;
        this.y = y;
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
    public int hashCode() {
        return Objects.hash(this.x, this.y);
    }

    @Override
    public boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Point) {
            final Point p = (Point) obj;
            match = Double.compare(p.getX(), this.x) == 0
                    && Double.compare(p.getY(), this.y) == 0;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return this.textValue;
    }


}
