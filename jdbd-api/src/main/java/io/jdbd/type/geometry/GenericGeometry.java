package io.jdbd.type.geometry;

public interface GenericGeometry {

    /**
     * @return wkb
     * @throws IllegalStateException when content too long.
     */
    byte[] toWkb(boolean bigEndian) throws IllegalStateException;


    /**
     * @return wkt
     * @throws IllegalStateException when content too long.
     */
    String toWkt() throws IllegalStateException;


    /**
     * @return database vendor output( if too long omit suffix part)
     */
    @Override
    String toString();


}
