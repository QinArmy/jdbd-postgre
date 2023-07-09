package io.jdbd.type.geo;

import io.jdbd.type.Point;
import org.reactivestreams.Publisher;

import java.util.List;

@Deprecated
public interface LineString extends LongGeometry {

    Publisher<Point> points();

    List<Point> pointList();

}
