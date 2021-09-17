package io.jdbd.type.geo;

import io.jdbd.type.geometry.Point;

public interface Line extends LineString {

    Point getPoint1();

    Point getPoint2();

}
