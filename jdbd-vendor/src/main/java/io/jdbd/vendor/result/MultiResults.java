package io.jdbd.vendor.result;

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


    public static MultiResult error(Throwable e) {
        return new ErrorMultiResult(e);
    }

    public static ReactorMultiResult create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return ReactorMultiResults.create(adjutant, callback);
    }

    public static OrderedFlux orderedFluxError(Throwable error) {
        return new OrderedFluxError(error);
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
    public static Mono<ResultStates> update_0(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return UpdateResultSubscriber_0.create(adjutant, callback);
    }

    public static Mono<ResultStates> update(Consumer<FluxResultSink> callback) {
        return UpdateResultSubscriber.create(callback);
    }

    public static Flux<ResultRow> query(Consumer<ResultStates> stateConsumer
            , Consumer<FluxResultSink> callback) {
        return QueryResultSubscriber.create(stateConsumer, callback);
    }

    public static Flux<ResultStates> batchUpdate(Consumer<FluxResultSink> consumer) {
        return BatchUpdateResultSubscriber.create(consumer);
    }

    public static MultiResult asMulti(ITaskAdjutant adjutant, Consumer<FluxResultSink> consumer) {
        return MultiResultSubscriber.create(adjutant, consumer);
    }

    public static OrderedFlux asFlux(Consumer<FluxResultSink> consumer) {
        return FluxResult.create(consumer);
    }


}
