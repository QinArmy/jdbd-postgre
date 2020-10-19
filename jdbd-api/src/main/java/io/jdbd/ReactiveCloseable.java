package io.jdbd;

import reactor.core.publisher.Mono;

public interface ReactiveCloseable {

    Mono<Void> close();
}
