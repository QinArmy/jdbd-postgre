package io.jdbd;

import org.reactivestreams.Publisher;

public interface ReactiveCloseable {

    Publisher<Void> close();
}
