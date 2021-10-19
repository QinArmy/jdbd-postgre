package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

/**
 * @see Line
 */
public interface LineString extends Curve, LongGenericGeometry {

    Publisher<Point> points();


}
