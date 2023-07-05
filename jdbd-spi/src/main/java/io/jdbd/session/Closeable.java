package io.jdbd.session;

import org.reactivestreams.Publisher;

public interface Closeable {

    Publisher<Void> close();
}
