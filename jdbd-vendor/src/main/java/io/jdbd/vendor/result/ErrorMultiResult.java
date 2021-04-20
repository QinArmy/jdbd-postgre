package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

final class ErrorMultiResult implements ReactorMultiResult {

    private final JdbdException error;

    ErrorMultiResult(JdbdException error) {
        this.error = error;
    }

    @Override
    public final Mono<ResultStatus> nextUpdate() {
        return Mono.error(this.error);
    }

    @Override
    public final Flux<ResultRow> nextQuery(Consumer<ResultStatus> statesConsumer) {
        return Flux.error(this.error);
    }

    @Override
    public final Flux<ResultRow> nextQuery() {
        return Flux.error(this.error);
    }


}
