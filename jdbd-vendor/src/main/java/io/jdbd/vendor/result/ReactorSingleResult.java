package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public interface ReactorSingleResult extends SingleResult {

    @Override
    Mono<ResultStatus> receiveUpdate();

    @Override
    Flux<ResultRow> receiveQuery(Consumer<ResultStatus> statesConsumer);

    @Override
    Flux<ResultRow> receiveQuery();


}
