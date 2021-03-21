package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

import java.util.List;

/**
 *
 */
public interface LineString extends Curve {

    int BOUNDARY_POINT_LIST_SIZE = ((1 << 30) - 12) / (22 * 2 + 2);
    // max wkb byte count / Point.WKB_BYTES
    int MAX_POINT_LIST_SIZE = ((1 << 30) - 9) / Point.WKB_BYTES;

    /**
     * WKB-TYPE linestring,unsigned int.
     */
    byte WKB_TYPE_LINE_STRING = 2;


    /**
     * @return a unmodifiable list ,{@link List#size()} always great or equals {@code 2}.
     */
    List<Point> pointList() throws IllegalStateException;

    Publisher<Point> pointStream();


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
