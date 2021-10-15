package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

final class ErrorMultiResult implements ReactorMultiResult {

    private final Throwable error;

    ErrorMultiResult(Throwable error) {
        this.error = error;
    }

    @Override
    public Mono<ResultStates> nextUpdate() {
        return Mono.error(this.error);
    }

    @Override
    public Flux<ResultRow> nextQuery(Consumer<ResultStates> statesConsumer) {
        return Flux.error(this.error);
    }

    @Override
    public Flux<ResultRow> nextQuery() {
        return Flux.error(this.error);
    }


}
