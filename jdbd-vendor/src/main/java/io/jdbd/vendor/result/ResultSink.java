package io.jdbd.vendor.result;


import io.jdbd.result.Result;

/**
 * @see ResultSetReader
 */
public interface ResultSink {

    boolean isCancelled();

    void next(Result result);

}
