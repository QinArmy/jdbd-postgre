package io.jdbd.type.geometry;

public interface Point extends Geometry {

    double getX();

    double getY();

    byte[] toWkb(boolean bigEndian);

    String toWkt();

    /**
     * @return wkt or database vendor output
     */
    @Override
    String toString();

}
