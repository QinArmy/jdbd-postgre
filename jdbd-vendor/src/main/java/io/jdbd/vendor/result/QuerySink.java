package io.jdbd.vendor.result;

import io.jdbd.result.ResultStatus;

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
    void accept(ResultStatus resultStatus);


    void complete();

}
