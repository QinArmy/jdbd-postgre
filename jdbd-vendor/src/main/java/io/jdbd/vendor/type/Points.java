package io.jdbd.vendor.type;

import io.jdbd.type.Point;

public abstract class Points {

    protected Points() {
        throw new UnsupportedOperationException();
    }

    public static Point point(double x, double y) {
        return PointImpl.create(x, y);
    }


}
