package io.jdbd.type;

import java.io.IOException;
import java.nio.channels.FileChannel;

@Deprecated
public interface LongBinary {

    boolean isArray();

    byte[] asArray() throws IllegalStateException;

    /**
     * <p>
     * equivalence {@code openReadOnlyChannel(true)}
     * </p>
     */
    FileChannel openReadOnlyChannel() throws IOException, IllegalStateException;


}
