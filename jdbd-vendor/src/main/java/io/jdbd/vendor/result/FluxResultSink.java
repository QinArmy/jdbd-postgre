package io.jdbd.vendor.result;

public interface FluxResultSink extends ResultSink {

    void error(Throwable e);

    void complete();

    ResultSink froResultSet();

}
