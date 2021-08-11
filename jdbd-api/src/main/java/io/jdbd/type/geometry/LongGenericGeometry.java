package io.jdbd.type.geometry;

import io.jdbd.type.LongBinary;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface LongGenericGeometry extends GenericGeometry, LongBinary {

    /**
     * @return WKB bytes
     */
    @Override
    byte[] asArray() throws IllegalStateException;

    /**
     * @return WKB bytes {@link FileChannel} of underlying file.
     */
    @Override
    FileChannel openReadOnlyChannel() throws IOException, IllegalStateException;


}
