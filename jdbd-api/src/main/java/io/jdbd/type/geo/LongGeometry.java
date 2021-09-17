package io.jdbd.type.geo;

import io.jdbd.type.geometry.Geometry;
import org.reactivestreams.Publisher;

public interface LongGeometry extends Geometry {

    boolean hasUnderlyingFile();

    Publisher<byte[]> wkb();

    /**
     * @return {@link Publisher} byte array with {@link java.nio.charset.StandardCharsets#US_ASCII}.
     */
    Publisher<byte[]> wkt();

}
