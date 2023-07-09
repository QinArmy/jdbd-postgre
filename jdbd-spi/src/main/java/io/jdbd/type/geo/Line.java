package io.jdbd.type.geo;

import io.jdbd.type.Point;

@Deprecated
public interface Line extends LineString {

    Point getPoint1();

    Point getPoint2();

}
