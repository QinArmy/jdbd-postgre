package io.jdbd.type;

import org.reactivestreams.Publisher;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;

public interface Text {

    boolean isString();

    String asString() throws IllegalStateException;

    Publisher<CharBuffer> asStream();

    FileChannel openReadOnlyChannel() throws IOException;

}
