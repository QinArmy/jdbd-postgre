package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.task.TaskAdjutant;
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
        return MultiResultCreate.create(adjutant, callback);
    }

    public static Flux<SingleResult> createAsFlux(TaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        throw new UnsupportedOperationException();
    }


}
