package io.jdbd.type.geometry;

/**
 * @see SmallLineString
 * @see LargeLineString
 */
public interface LineString extends Curve {

    /**
     * WKB-TYPE linestring,unsigned int.
     */
    int WKB_TYPE_LINE_STRING = 2;


    Point startPoint();

    Point endPoint();

    /**
     * @return true: {@link #startPoint()} equals {@link #endPoint()}
     */
    boolean isClosed();

    /**
     * @return true:it consists of exactly two points.
     */
    boolean isLine();


}
