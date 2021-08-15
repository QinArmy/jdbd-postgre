package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@Deprecated
public interface ReactorSingleResult extends SingleResult {

    @Override
    Mono<ResultState> receiveUpdate();

    @Override
    Flux<ResultRow> receiveQuery(Consumer<ResultState> statesConsumer);

    @Override
    Flux<ResultRow> receiveQuery();


}
