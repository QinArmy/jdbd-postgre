package io.jdbd.vendor.result;


import io.jdbd.result.ResultItem;

public interface ResultSink {

    boolean isCancelled();

    void next(ResultItem result);

    void error(Throwable e);

    void complete();

}
