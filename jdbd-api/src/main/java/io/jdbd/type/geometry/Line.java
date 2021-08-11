package io.jdbd.type.geometry;

/**
 * This representing LINE that is special LINESTRING (it consists of exactly two points.).
 */
public interface Line extends LineString {

    Point getPoint1();

    Point getPoint2();

    /**
     * @return WKT of LINESTRING.
     */
    @Override
    String toString();

}
