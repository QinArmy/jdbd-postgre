package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.stmt.ExecutableStatement;
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
     * @see ExecutableStatement#executeQuery(Consumer)
     */
    void accept(ResultStatus resultStatus);

}
