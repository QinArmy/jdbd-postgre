package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.SmallLineString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class DefaultSmallLineString implements SmallLineString {

    static SmallLineString create(List<Point> pointList) {
        final SmallLineString lineString;
        final int size = pointList.size();
        if (size == 2) {
            lineString = new DefaultLine(pointList);
        } else if (size > 2 && size < SmallLineString.MAX_POINT_LiST_SIZE) {
            lineString = new DefaultSmallLineString(pointList);
        } else {
            throw new IllegalArgumentException(String.format(
                    "pointList size[%s] not in [2,%s]", pointList.size(), SmallLineString.MAX_POINT_LiST_SIZE));
        }
        return lineString;
    }

    static LineString line(Point one, Point two) {
        List<Point> list = new ArrayList<>(2);
        list.add(one);
        list.add(two);
        return new DefaultLine(list);
    }

    private final List<Point> pointList;

    private DefaultSmallLineString(List<Point> pointList) {
        this.pointList = Collections.unmodifiableList(pointList);
    }


    @Override
    public String asWkt() {
        int capacity = 10 + this.pointList.size() * 14;
        return Geometries.lineStringAsWkt(this, new StringBuilder(capacity))
                .toString();
    }

    @Override
    public List<Point> pointList() {
        return this.pointList;
    }

    @Override
    public Point startPoint() {
        return this.pointList.get(0);
    }

    @Override
    public Point endPoint() {
        return this.pointList.get(this.pointList.size() - 1);
    }

    @Override
    public boolean isClosed() {
        return startPoint().equals(endPoint());
    }

    @Override
    public boolean isLine() {
        return this.pointList.size() == 2;
    }

    @Override
    public int hashCode() {
        return this.pointList.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof SmallLineString) {
            match = this.pointList.equals(((SmallLineString) obj).pointList());
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public byte[] asWkbBytes(final boolean bigEndian) {
        final byte[] wkbBytes = new byte[9 + this.pointList.size() * Point.WKB_BYTES];
        Geometries.lineStringAsWkbBytes(this.pointList, bigEndian, wkbBytes, 0);
        return wkbBytes;
    }

    @Override
    public String toString() {
        return asWkt();
    }


    private static final class DefaultLine extends DefaultSmallLineString implements Line {

        private DefaultLine(List<Point> pointList) {
            super(pointList);
            if (pointList.size() != 2) {
                throw new IllegalArgumentException("pointList size isn't 2.");
            }
        }

    }


}
