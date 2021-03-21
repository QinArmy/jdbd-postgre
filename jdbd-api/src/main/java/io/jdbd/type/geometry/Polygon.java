package io.jdbd.type.geometry;

import java.util.Collection;

/**
 *
 */
public interface Polygon extends Geometry, Surface {

    /**
     * WKB-TYPE polygon,unsigned int.
     */
    int WKB_TYPE_POLYGON = 3;

    /**
     * @return a unmodifiable Collection.
     */
    Collection<LinearRing> linearRings();

}
