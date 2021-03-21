package io.jdbd.type.geometry;

/**
 * @see Point
 * @see PointZ
 * @see PointM
 */
public interface PointZM extends GenericPoint {

    /**
     * WKB-TYPE POINTZM,unsigned int.
     */
    int WKB_TYPE_POINT_ZM = 3001;

    byte WKB_BYTES = 37;

    double getZ();

    double getM();


}
