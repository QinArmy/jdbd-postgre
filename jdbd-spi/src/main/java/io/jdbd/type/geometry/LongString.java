package io.jdbd.type.geometry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * This interface adapter of {@link String} or large string.
 */
@Deprecated
public interface LongString {

    boolean isString();

    String asString() throws IllegalStateException;

    Charset charset() throws IllegalStateException;

    /**
     * @return {@link FileChannel} that is read only and underlying file will be deleted when {@link FileChannel} close.
     * @throws IOException io error
     */
    FileChannel openReadOnlyChannel() throws IOException;


}
