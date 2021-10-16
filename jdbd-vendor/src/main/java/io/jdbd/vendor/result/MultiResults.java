package io.jdbd.vendor.result;

import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public abstract class MultiResults {

    protected MultiResults() {
        throw new UnsupportedOperationException();
    }


    public static MultiResult error(Throwable e) {
        return new ErrorMultiResult(e);
    }


    public static OrderedFlux fluxError(Throwable error) {
        return new OrderedFluxError(error);
    }


    public static Mono<ResultStates> update(Consumer<ResultSink> callback) {
        return UpdateResultSubscriber.create(callback);
    }

    public static Flux<ResultRow> query(Consumer<ResultStates> stateConsumer
            , Consumer<ResultSink> callback) {
        return QueryResultSubscriber.create(stateConsumer, callback);
    }

    public static Flux<ResultStates> batchUpdate(Consumer<ResultSink> consumer) {
        return BatchUpdateResultSubscriber.create(consumer);
    }

    public static MultiResult asMulti(ITaskAdjutant adjutant, Consumer<ResultSink> consumer) {
        return MultiResultSubscriber.create(adjutant, consumer);
    }

    public static OrderedFlux asFlux(Consumer<ResultSink> consumer) {
        return FluxResult.create(consumer);
    }


}
