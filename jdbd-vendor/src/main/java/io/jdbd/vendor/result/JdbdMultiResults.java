package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.vendor.MultiResultsSink;
import io.jdbd.vendor.TaskAdjutant;

import java.util.function.Consumer;

public abstract class JdbdMultiResults {

    protected JdbdMultiResults() {
        throw new UnsupportedOperationException();
    }


    public static ReactorMultiResults error(JdbdException e) {
        return new ErrorMultiResults(e);
    }

    public static ReactorMultiResults create(TaskAdjutant adjutant, Consumer<MultiResultsSink> callback) {
        return MultiResultsCreate.create(adjutant, callback);
    }


}
