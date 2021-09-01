package io.jdbd.vendor.result;

import io.jdbd.result.ResultStates;

/**
 * @see MultiResultSink#nextQuery()
 */
@Deprecated
public interface QuerySink extends ResultRowSink_0 {

    /**
     * <p>
     * invoke after {@link #complete()}
     * </p>
     */
    @Override
    void accept(ResultStates resultStates);


    void complete();

}
