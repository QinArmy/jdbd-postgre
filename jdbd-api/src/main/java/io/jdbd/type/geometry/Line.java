package io.jdbd.type.geometry;

/**
 * This representing LINE that is special LINESTRING (it consists of exactly two points.).
 * <p>
 * JDBD statement bind method not don't support this type,only supported by {@link io.jdbd.result.ResultRow}.
 * </p>
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
