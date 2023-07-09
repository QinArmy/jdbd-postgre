package io.jdbd.postgre.type;

import io.jdbd.type.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-POLYGON">Polygons</a>
 */
public final class PgPolygon {

    /**
     * @param value format: ( ( x1 , y1 ) , ... , ( xn , yn ) )
     */
    static PgPolygon from(String value) {
        value = value.trim();
        value = value.trim();
        if (value.length() < 2) {
            throw new IllegalArgumentException("format error");
        }
        final char first = value.charAt(0), last = value.charAt(value.length() - 1);
        if ((first == '(' && last == ')')) {
            final List<Point> pointList = new ArrayList<>();
            final int newIndex;
            newIndex = PgGeometries.readPoints(value, 1, pointList::add);
            PgGeometries.checkPgGeometricSuffix(value, newIndex);
            return new PgPolygon(value, pointList);
        }
        throw new IllegalArgumentException("format error");
    }

    private final String value;

    private final List<Point> pointList;

    private PgPolygon(String value, List<Point> pointList) {
        this.value = value;
        this.pointList = Collections.unmodifiableList(pointList);
    }


    public final List<Point> getPointList() {
        return pointList;
    }


    @Override
    public final int hashCode() {
        return Objects.hash(this.pointList);
    }

    @Override
    public final boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof PgPolygon) {
            match = this.pointList.equals(((PgPolygon) obj).pointList);
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
