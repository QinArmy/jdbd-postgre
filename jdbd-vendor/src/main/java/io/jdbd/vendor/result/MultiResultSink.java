package io.jdbd.vendor.result;

import io.jdbd.result.ResultStatus;


public interface MultiResultSink {

    boolean isCancelled();

    /**
     *
     */
    void error(Throwable e);

    /**
     *
     */
    void nextUpdate(ResultStatus resultStatus);

    /**
     *
     */
    QuerySink nextQuery();

    /**
     */
    void complete();

}
