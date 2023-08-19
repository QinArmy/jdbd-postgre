package io.jdbd.postgre.type;

import io.jdbd.type.Point;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.8">Boxes</a>
 */
public final class PgBox {

    /**
     * @param value format:( x1 , y1 ) , ( x2 , y2 )
     */
    static PgBox from(String value) {
        value = value.trim();
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
        newIndex = PgGeometries.readPoints(value, 0, consumer);
        PgGeometries.checkPgGeometricSuffix(value, newIndex);
        return new PgBox(value, pointArray[0], pointArray[1]);
    }

    private final String value;

    private final Point point1;

    private final Point point2;

    private PgBox(String value, Point point1, Point point2) {
        this.value = value;
        this.point1 = point1;
        this.point2 = point2;
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
        } else if (obj instanceof PgBox) {
            final PgBox v = (PgBox) obj;
            match = (this.point1.equals(v.point1) && this.point2.equals(v.point2))
                    || isSwapped(v)
                    || isOppositeWithoutWrapped(v)
                    || isOppositeWithSwapped(v);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.value;
    }

    private boolean isSwapped(PgBox v) {
        return this.point1.equals(v.point2) && this.point2.equals(v.point1);
    }

    private boolean isOppositeWithoutWrapped(PgBox v) {
        // Using the opposite two points of the box:
        // (x1,y1),(x2,y2) -> (x1,y2),(x2,y1)
        return Double.compare(this.point1.getX(), v.point1.getX()) == 0
                && Double.compare(this.point1.getY(), v.point2.getY()) == 0
                && Double.compare(this.point2.getX(), v.point2.getX()) == 0
                && Double.compare(this.point2.getY(), v.point1.getY()) == 0;
    }

    private boolean isOppositeWithSwapped(PgBox v) {
        // Using the opposite two points of the box, and the points are swapped
        // (x1,y1),(x2,y2) -> (x2,y1),(x1,y2)
        return Double.compare(this.point1.getX(), v.point2.getX()) == 0
                && Double.compare(this.point1.getY(), v.point1.getY()) == 0
                && Double.compare(this.point2.getX(), v.point1.getX()) == 0
                && Double.compare(this.point2.getY(), v.point2.getY()) == 0;
    }


}
