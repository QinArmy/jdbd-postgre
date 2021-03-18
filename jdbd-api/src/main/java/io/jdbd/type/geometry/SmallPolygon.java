package io.jdbd.type.geometry;

import java.util.Collection;

public interface SmallPolygon extends Polygon, SmallGeometry {

    Collection<LinearRing> linearRings();

}
