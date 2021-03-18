package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

public interface LargeLineString extends LineString, LargeGeometry {

    Publisher<Point> pointStream();

}
