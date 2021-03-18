package io.jdbd.type.geometry;


public interface Point extends SmallGeometry {

    /**
     * WKB-TYPE point,unsigned int.
     */
    int WKB_TYPE_POINT = 1;

    int WKB_BYTES = 21;

    double getX();

    double getY();

    int getTextLength();

}
