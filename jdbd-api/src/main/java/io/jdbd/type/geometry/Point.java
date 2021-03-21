package io.jdbd.type.geometry;


/**
 * @see PointZ
 * @see PointM
 * @see PointZM
 */
public interface Point extends GenericPoint {

    /**
     * WKB-TYPE point,unsigned int.
     */
    byte WKB_TYPE_POINT = 1;

    byte WKB_BYTES = 21;

    byte getPointTextLength();


}
