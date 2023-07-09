package io.jdbd.type.geometry;

import io.jdbd.type.Point;
import org.reactivestreams.Publisher;

/**
 * @see Line
 */
@Deprecated
public interface LineString extends Curve, LongGenericGeometry {

    Publisher<Point> points();


}
