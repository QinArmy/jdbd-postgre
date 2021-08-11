package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

/**
 * @see Line
 */
public interface LineString extends Curve, LongGenericGeometry {

    Publisher<Point> points();

    /**
     * @return <ul>
     *     <li>{@link #isArray()} is {@code true} : return WKT of LINESTRING</li>
     *     <li>or : return WKT of LINESTRING that omit points after 10 points</li>
     * </ul>
     */
    @Override
    String toString();


}
