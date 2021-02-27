package io.jdbd.vendor.result;

import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public interface ReactorMultiResults extends MultiResults {

    @Override
    Mono<ResultStates> nextUpdate();

    @Override
    Flux<ResultRow> nextQuery(Consumer<ResultStates> statesConsumer);

    @Override
    Flux<ResultRow> nextQuery();
}
