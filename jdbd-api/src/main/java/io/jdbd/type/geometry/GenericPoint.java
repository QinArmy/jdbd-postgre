package io.jdbd.type.geometry;

/**
 * @see Point
 * @see PointZ
 * @see PointM
 * @see PointZM
 */
public interface GenericPoint extends Geometry {

    double getX();

    double getY();

    /**
     * @return always true.
     */
    @Override
    boolean isSmall();

}
