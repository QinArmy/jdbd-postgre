package io.jdbd.type.geometry;

import io.jdbd.type.Point;

@Deprecated
public interface Circle {

    Point getCenter();

    double getRadius();


}
