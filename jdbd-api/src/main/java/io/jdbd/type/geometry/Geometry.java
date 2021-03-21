package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

import java.io.IOException;
import java.nio.file.Path;

/**
 * <p>
 * non-instantiable
 * </p>
 *
 * @see Point
 * @see LineString
 */
public interface Geometry {

    byte[] asWkb(boolean bigEndian) throws IllegalStateException;


    /**
     * @return WKT format.
     */
    String asWkt() throws IllegalStateException;

    boolean isMemory();

    /**
     * @return a unsigned int .
     */
    int elementCount();

    long elementCountAsLong();

    byte[] geometryMd5(boolean bigEndian) throws IOException;

    @Override
    boolean equals(Object o);

    long getTextLength();

    long getWkbLength();

    Publisher<byte[]> asWkbStream(boolean bigEndian);

    Publisher<String> asWktStream();

    void asWkbToPath(boolean bigEndian, Path path) throws IOException;

    void asWktToPath(Path path) throws IOException;

    /**
     * @return exists underlying file and delete success.
     */
    boolean deleteIfExists() throws IOException;


}
