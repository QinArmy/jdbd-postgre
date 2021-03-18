package io.jdbd.type.geometry;

import org.reactivestreams.Publisher;

import java.io.IOException;
import java.nio.file.Path;

/**
 * <p>
 * This interface representing a {@link Geometry} that cannot be expressed
 * by {@link String} with WKT(Well-Known Text (WKT) Format),because it too large.
 * </p>
 */
public interface LargeGeometry extends Geometry {

    default Publisher<byte[]> asWkbStream(boolean bigEndian) {
        return null;
    }

    default Publisher<char[]> asWktStream(boolean bigEndian) {
        return null;
    }

    void asWkbToPath(boolean bigEndian, Path path) throws IOException;

    void asWktToPath(Path path) throws IOException;


}
