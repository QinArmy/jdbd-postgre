package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SingleResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@Deprecated
public interface ReactorSingleResult extends SingleResult {

    @Override
    Mono<ResultStates> receiveUpdate();

    @Override
    Flux<ResultRow> receiveQuery(Consumer<ResultStates> statesConsumer);

    @Override
    Flux<ResultRow> receiveQuery();


}
