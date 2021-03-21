package io.jdbd.type.geometry;


/**
 * @see Point
 * @see PointZ
 * @see PointZM
 */
public interface PointM extends GenericPoint {

    /**
     * WKB-TYPE POINTM,unsigned int.
     */
    int WKB_TYPE_POINT_M = 2001;

    byte WKB_BYTES = 29;

    double getM();

}
