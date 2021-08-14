package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public abstract class JdbdMultiResults {

    protected JdbdMultiResults() {
        throw new UnsupportedOperationException();
    }


    public static ReactorMultiResult error(JdbdException e) {
        return new ErrorMultiResult(e);
    }

    public static ReactorMultiResult create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return ReactorMultiResults.create(adjutant, callback);
    }


    /**
     * <p>
     * {@link MultiResultSink} isn't thread safe ,must invoke in {@link io.netty.channel.EventLoop}.
     * </p>
     */
    public static Flux<SingleResult> createAsFlux(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }

        });
    }

    @Deprecated
    public static Mono<ResultState> update_0(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return UpdateResultSubscriber_0.create(adjutant, callback);
    }

    public static Mono<ResultState> update(ITaskAdjutant adjutant, Consumer<FluxResultSink> callback) {
        return UpdateResultSubscriber.create(adjutant, callback);
    }

    public static Flux<ResultRow> query(ITaskAdjutant adjutant, Consumer<ResultState> stateConsumer
            , Consumer<MultiResultSink> callback) {
        return QueryResultSubscriber_0.create(adjutant, stateConsumer, callback);
    }

    public static Flux<ResultState> batchUpdate(ITaskAdjutant adjutant, Consumer<FluxSink<Result>> consumer) {
        return BatchUpdateResultSubscriber.create(adjutant, consumer);
    }


}
