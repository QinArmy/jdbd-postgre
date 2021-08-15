package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.*;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public abstract class MultiResults {

    protected MultiResults() {
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
    @Deprecated
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
            , Consumer<FluxResultSink> callback) {
        return QueryResultSubscriber.create(adjutant, stateConsumer, callback);
    }

    public static Flux<ResultState> batchUpdate(ITaskAdjutant adjutant, Consumer<FluxResultSink> consumer) {
        return BatchUpdateResultSubscriber.create(adjutant, consumer);
    }

    public static MultiResult asMulti(ITaskAdjutant adjutant, Consumer<FluxResultSink> consumer) {
        return MultiResultSubscriber.create(adjutant, consumer);
    }

    public static Flux<Result> asFlux(ITaskAdjutant adjutant, Consumer<FluxResultSink> consumer) {
        return Flux.create(sink -> Flux.from(FluxResult.create(adjutant, consumer))
                .doOnNext(sink::next)
                .doOnError(sink::error)
                .doOnComplete(sink::complete)
                .subscribe()
        );
    }


}
