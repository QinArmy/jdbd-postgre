package io.jdbd.type.geometry;

public interface Circle {

    Point getCenter();

    double getRadius();

    /**
     * @return format : Circle(center:x,y;radius:r)
     */
    @Override
    String toString();

}
