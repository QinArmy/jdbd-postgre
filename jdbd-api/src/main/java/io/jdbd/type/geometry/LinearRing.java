package io.jdbd.type.geometry;

public interface LinearRing extends LineString, Geometry {

    /**
     * @return always true.
     */
    @Override
    boolean isClosed();


}
