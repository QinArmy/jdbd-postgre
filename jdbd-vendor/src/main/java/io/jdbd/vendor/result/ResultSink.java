package io.jdbd.vendor.result;


import io.jdbd.result.Result;

public interface ResultSink {

    boolean isCancelled();

    void next(Result result);

    void error(Throwable e);

    void complete();

}
