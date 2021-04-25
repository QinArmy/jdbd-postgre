package io.jdbd.vendor.result;

import io.jdbd.result.ResultState;

/**
 * @see MultiResultSink#nextQuery()
 */
public interface QuerySink extends ResultRowSink {

    /**
     * <p>
     * invoke after {@link #complete()}
     * </p>
     */
    @Override
    void accept(ResultState resultState);


    void complete();

}
