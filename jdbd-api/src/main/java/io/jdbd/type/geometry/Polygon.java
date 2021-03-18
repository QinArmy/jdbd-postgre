package io.jdbd.type.geometry;

public interface Polygon extends Geometry, Surface {

    /**
     * WKB-TYPE polygon,unsigned int.
     */
    int WKB_TYPE_POLYGON = 3;

}
