package io.jdbd.type.geometry;

/**
 * @see Point
 * @see PointM
 * @see PointZM
 */
public interface PointZ extends GenericPoint {

    /**
     * WKB-TYPE POINTZ,unsigned int.
     */
    int WKB_TYPE_POINT_Z = 1001;

    byte WKB_BYTES = 29;

    double getZ();


}
