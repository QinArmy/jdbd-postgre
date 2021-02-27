package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

final class ErrorMultiResults implements ReactorMultiResults {

    private final JdbdException error;

    ErrorMultiResults(JdbdException error) {
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
