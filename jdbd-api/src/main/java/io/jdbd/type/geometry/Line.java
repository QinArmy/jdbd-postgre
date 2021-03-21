package io.jdbd.type.geometry;

import java.util.List;

public interface Line extends LineString, Geometry {

    /**
     * @return a unmodifiable list ,{@link List#size()} always  equals {@code 2}.
     */
    @Override
    List<Point> pointList();

}
