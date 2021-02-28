package io.jdbd.vendor.result;

import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import reactor.core.publisher.FluxSink;

import java.util.function.Consumer;

public interface ResultRowSink {

    /**
     * @see reactor.core.publisher.FluxSink#next(Object)
     */
    void next(ResultRow resultRow);

    /**
     * @see FluxSink#isCancelled()
     */
    boolean isCancelled();

    /**
     * @see io.jdbd.GenericStatement#executeQuery(Consumer)
     */
    void accept(ResultStates resultStates) throws IllegalStateException;

}
