package io.jdbd.type.geometry;

public interface Geometry {


    byte[] asWKB(boolean bigEndian);


    /**
     * @return WKT format.
     */
    @Override
    String toString();

}
