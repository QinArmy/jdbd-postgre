package io.jdbd.type.geometry;

/**
 * <p>
 * This interface representing a {@link Geometry} that can be expressed
 * by {@link String} with WKT(Well-Known Text (WKT) Format).
 * </p>
 *
 * @see SmallLineString
 * @see SmallPolygon
 */
public interface SmallGeometry extends Geometry {

    byte[] asWkbBytes(boolean bigEndian);


    /**
     * @return WKT format.
     */
    String asWkt();


}
