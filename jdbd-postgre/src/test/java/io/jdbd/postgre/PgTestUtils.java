package io.jdbd.postgre;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class PgTestUtils {

    public static <T> Mono<T> updateNoResponse() {
        return Mono.defer(() -> Mono.error(new RuntimeException("update no response")));
    }

    public static <T> Flux<T> queryNoResponse() {
        return Flux.defer(() -> Flux.error(new RuntimeException("update no response")));
    }


}
