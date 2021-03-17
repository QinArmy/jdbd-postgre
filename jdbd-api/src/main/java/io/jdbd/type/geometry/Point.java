package io.jdbd.type.geometry;

/**
 * @see Geometries#point(double, double)
 * @see Geometries#pointFromWKB(byte[], int)
 * @see Geometries#pointFromWKT(String)
 */
public interface Point extends Geometry {

    double getX();

    double getY();

}
