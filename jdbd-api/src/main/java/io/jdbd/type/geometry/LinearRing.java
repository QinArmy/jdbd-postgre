package io.jdbd.type.geometry;

public interface LinearRing extends SmallLineString {

    /**
     * @return always true.
     */
    @Override
    boolean isClosed();


}
