package io.jdbd.vendor.result;

import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.vendor.MultiResultsSink;
import reactor.core.publisher.FluxSink;

/**
 * @see MultiResultsSink#nextQuery()
 */
public interface QuerySink {

    @Deprecated
    FluxSink<ResultRow> getSink();

    default void next(ResultRow resultRow) {

    }

    default boolean isCanceled() {
        return false;
    }

    default void complete() {

    }


    /**
     * <p>
     *
     * </p>
     *
     * @throws IllegalStateException when duplicate invoke.
     */
    void acceptStatus(ResultStates resultStates) throws IllegalStateException;
}
