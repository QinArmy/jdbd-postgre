package io.jdbd.vendor.result;

import io.jdbd.result.ResultStates;

@Deprecated
public interface MultiResultSink {

    boolean isCancelled();

    /**
     *
     */
    void error(Throwable e);

    /**
     *
     */
    void nextUpdate(ResultStates resultStates);

    /**
     *
     */
    QuerySink nextQuery();

    /**
     */
    void complete();

}
