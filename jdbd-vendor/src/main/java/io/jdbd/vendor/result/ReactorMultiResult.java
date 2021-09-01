package io.jdbd.vendor.result;

import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@Deprecated
public interface ReactorMultiResult extends MultiResult {


    @Override
    Mono<ResultStates> nextUpdate();

    @Override
    Flux<ResultRow> nextQuery(Consumer<ResultStates> statesConsumer);

    @Override
    Flux<ResultRow> nextQuery();
}
