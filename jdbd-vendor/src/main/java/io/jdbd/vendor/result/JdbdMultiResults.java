package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.vendor.task.TaskAdjutant;

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


}
