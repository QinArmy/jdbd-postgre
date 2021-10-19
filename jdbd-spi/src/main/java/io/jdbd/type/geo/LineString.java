package io.jdbd.type.geo;

import io.jdbd.type.geometry.Point;
import org.reactivestreams.Publisher;

import java.util.List;

public interface LineString extends LongGeometry {

    Publisher<Point> points();

    List<Point> pointList();

}
