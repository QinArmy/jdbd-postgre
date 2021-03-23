package io.jdbd.type;

import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

public interface Blob {

    boolean isArray();

    byte[] asArray() throws IllegalStateException;

    Publisher<ByteBuffer> asStream();

}
