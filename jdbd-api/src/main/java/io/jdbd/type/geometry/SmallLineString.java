package io.jdbd.type.geometry;

import java.util.List;

public interface SmallLineString extends LineString, SmallGeometry {

    int MAX_POINT_LiST_SIZE = ((1 << 30) - 12) / (22 * 2 + 2);

    /**
     * @return a unmodifiable list ,{@link List#size()} always great or equals {@code 2}.
     */
    List<Point> pointList();


}
