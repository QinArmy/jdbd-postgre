package io.jdbd.type;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface LongString {

    boolean isString();

    String asString() throws IllegalStateException;

    FileChannel openReadOnlyChannel() throws IOException;


}
