package io.jdbd.mysql.protocol.client;


import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface MultiResults {

    Mono<Long> nextUpdate(Consumer<ResultStates> consumer);

    <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> decoder, Consumer<ResultStates> consumer);

}
