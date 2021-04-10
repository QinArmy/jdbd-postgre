package io.jdbd.type;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface Text {

    boolean isString();

    String asString() throws IllegalStateException;

    FileChannel openReadOnlyChannel() throws IOException;

    FileChannel openReadOnlyChannel(boolean deleteOnClose) throws IOException;

}
