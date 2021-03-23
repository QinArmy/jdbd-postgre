package io.jdbd.type;

import org.reactivestreams.Publisher;

import java.nio.CharBuffer;

public interface Text {

    boolean isString();

    String asString() throws IllegalStateException;

    Publisher<CharBuffer> asStream();

}
