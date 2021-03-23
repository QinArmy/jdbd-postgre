package io.jdbd.type;

import org.reactivestreams.Publisher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Blob {

    boolean isArray();

    byte[] asArray() throws IllegalStateException;

    Publisher<ByteBuffer> asStream(boolean deleteOnComplete);

    FileChannel openReadOnlyChannel(boolean deleteOnClose) throws IOException;


}
