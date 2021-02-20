package io.jdbd.vendor.result;

import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import reactor.core.publisher.FluxSink;

public interface QuerySink {

    FluxSink<ResultRow> getSink();

    /**
     * @throws IllegalStateException when duplicate invoke.
     */
    void acceptStatus(ResultStates resultStates) throws IllegalStateException;
}
