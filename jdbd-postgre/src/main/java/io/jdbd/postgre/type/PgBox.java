package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.Geometries;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.8">Boxes</a>
 */
public final class PgBox {

    /**
     * @param textValue like ( x1 , y1 ) , ( x2 , y2 )
     */
    public static PgBox parse(final String textValue) {
        final String format = "Text[%s] isn't postgre box.";

        final Point[] points = new Point[2];
        final BiConsumer<Double, Double> pointConsumer = (x, y) -> {
            if (points[0] == null) {
                points[0] = Geometries.point(x, y);
            } else if (points[1] == null) {
                points[1] = Geometries.point(x, y);
            } else {
                throw new IllegalArgumentException(String.format(format, textValue));
            }
        };
        final int newIndex;
        newIndex = PgTypes.doReadPoints(textValue, 0, pointConsumer);

        if (points[1] == null) {
            throw new IllegalArgumentException(String.format(format, textValue));
        } else if (newIndex < textValue.length()) {
            for (int i = newIndex, end = textValue.length(); i < end; i++) {
                if (!Character.isWhitespace(textValue.charAt(i))) {
                    throw new IllegalArgumentException(String.format(format, textValue));
                }
            }
        }
        return new PgBox(points[0], points[1]);
    }

    public static PgBox create(Point point1, Point point2) {
        return new PgBox(point1, point2);
    }

    private final Point point1;

    private final Point point2;

    private PgBox(Point point1, Point point2) {
        this.point1 = point1;
        this.point2 = point2;
    }

    public final Point getPoint1() {
        return point1;
    }

    public final Point getPoint2() {
        return point2;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.point1, this.point2);
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof PgBox) {
            PgBox b = (PgBox) obj;
            match = b.point1.equals(this.point1)
                    && b.point2.equals(this.point2);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return new StringBuilder()
                .append("(")
                .append(this.point1.getX())
                .append(",")
                .append(this.point1.getY())
                .append(")")
                .append(",(")
                .append(this.point2.getX())
                .append(",")
                .append(this.point2.getY())
                .append(")")
                .toString();
    }


}
