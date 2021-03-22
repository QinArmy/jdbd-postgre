package io.jdbd.type.geometry;

/**
 * @see Point
 * @see PointZ
 * @see PointM
 * @see PointZM
 */
public interface GenericPoint extends Geometry {

    /**
     * length of coordinate number text and separator(space).
     * <p>
     * eg: <ul>
     * <li>0.0 1.33 , length = 8</li>
     * <li>0.0 1.33 2.4, length = 12</li>
     * </ul>
     * </p>
     */
    byte getPointTextLength();

    double getX();

    double getY();

    /**
     * @return always true.
     */
    @Override
    boolean isSmall();

}
