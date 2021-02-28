package io.jdbd.vendor.result;

import io.jdbd.ResultStates;
import io.jdbd.vendor.MultiResultsSink;

/**
 * @see MultiResultsSink#nextQuery()
 */
public interface QuerySink extends ResultRowSink {

    /**
     * <p>
     * invoke after {@link #complete()}
     * </p>
     */
    @Override
    void accept(ResultStates resultStates) throws IllegalStateException;


    void complete();

}
