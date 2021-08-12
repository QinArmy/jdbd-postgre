package io.jdbd.vendor.type;

import io.jdbd.type.geometry.*;
import io.jdbd.vendor.util.GeometryUtils;
import io.jdbd.vendor.util.JdbdNumbers;

public abstract class Geometries {

    protected Geometries() {
        throw new UnsupportedOperationException();
    }

    public static Point point(double x, double y) {
        return PointImpl.create(x, y);
    }

    public static Line line(Point point1, Point point2) {
        return LineImpl.create(point1, point2);
    }

    public static Line lineFromWkb(final byte[] wkb) {
        if (wkb.length != 41) {
            throw new IllegalArgumentException("Non LineString.");
        }
        if (WkbType.fromWkbArray(wkb, 0) != WkbType.LINE_STRING) {
            throw new IllegalArgumentException("Non LineString.");
        }
        int offset = 0;
        final boolean bigEndian = GeometryUtils.readEndian(wkb[offset]);
        offset += 5;
        if (JdbdNumbers.readIntFromEndian(bigEndian, wkb, offset, 4) != 2) {
            throw new IllegalArgumentException("Non LineString.");
        }
        offset += 4;
        double x, y;
        Point point;

        x = JdbdNumbers.readDoubleFromEndian(bigEndian, wkb, offset, 8);
        offset += 8;
        y = JdbdNumbers.readDoubleFromEndian(bigEndian, wkb, offset, 8);
        point = point(x, y);
        offset += 8;

        x = JdbdNumbers.readDoubleFromEndian(bigEndian, wkb, offset, 8);
        offset += 8;
        y = JdbdNumbers.readDoubleFromEndian(bigEndian, wkb, offset, 8);

        return LineImpl.create(point, point(x, y));
    }

    public static LineString lineStringFromWkb(final byte[] wkb) {
        return LineStrings.fromWkbBytes(wkb);
    }

    public static Circle circle(Point center, double radius) {
        return CircleImpl.create(center, radius);
    }

}
