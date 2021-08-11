package io.jdbd.type.geometry;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface LongString {

    boolean isString();

    String asString() throws IllegalStateException;

    /**
     * @return {@link FileChannel} that is read only and underlying file will be deleted when {@link FileChannel} close.
     * @throws IOException io error
     */
    FileChannel openReadOnlyChannel() throws IOException;


}
