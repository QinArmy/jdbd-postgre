package io.jdbd.type.geometry;

@Deprecated
public interface GenericGeometry {

    /**
     * @return wkb with big endian.
     * @throws IllegalStateException when content too long.
     */
    byte[] toWkb() throws IllegalStateException;


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
