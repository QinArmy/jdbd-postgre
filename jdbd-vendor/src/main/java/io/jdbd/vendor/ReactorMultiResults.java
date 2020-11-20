package io.jdbd.vendor;

import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface ReactorMultiResults extends MultiResults {


    @Override
    Mono<Long> nextUpdate(Consumer<ResultStates> statesConsumer);

    @Override
    <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder, Consumer<ResultStates> statesConsumer);


}
