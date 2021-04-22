package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.task.TaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

public abstract class JdbdMultiResults {

    protected JdbdMultiResults() {
        throw new UnsupportedOperationException();
    }


    public static ReactorMultiResult error(JdbdException e) {
        return new ErrorMultiResult(e);
    }

    public static ReactorMultiResult create(TaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return ReactorMultiResults.create(adjutant, callback);
    }


    /**
     * <p>
     * {@link MultiResultSink} isn't thread safe ,must invoke in {@link io.netty.channel.EventLoop}.
     * </p>
     */
    public static Flux<SingleResult> createAsFlux(TaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }

        });
    }


}
