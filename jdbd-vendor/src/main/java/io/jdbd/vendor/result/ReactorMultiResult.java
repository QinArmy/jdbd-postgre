package io.jdbd.vendor.result;

import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public interface ReactorMultiResult extends MultiResult {


    @Override
    Mono<ResultStatus> nextUpdate();

    @Override
    Flux<ResultRow> nextQuery(Consumer<ResultStatus> statesConsumer);

    @Override
    Flux<ResultRow> nextQuery();
}
