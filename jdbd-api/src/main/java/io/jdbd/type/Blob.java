package io.jdbd.type;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface Blob {

    boolean isArray();

    byte[] asArray() throws IllegalStateException;


    FileChannel openReadOnlyChannel() throws IOException;

    FileChannel openReadOnlyChannel(boolean deleteOnClose) throws IOException;


}
