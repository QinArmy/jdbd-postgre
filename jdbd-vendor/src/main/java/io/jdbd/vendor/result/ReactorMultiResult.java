package io.jdbd.vendor.result;

import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@Deprecated
public interface ReactorMultiResult extends MultiResult {


    @Override
    Mono<ResultState> nextUpdate();

    @Override
    Flux<ResultRow> nextQuery(Consumer<ResultState> statesConsumer);

    @Override
    Flux<ResultRow> nextQuery();
}
