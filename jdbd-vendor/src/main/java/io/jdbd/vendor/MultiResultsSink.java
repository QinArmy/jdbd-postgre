package io.jdbd.vendor;

import io.jdbd.ResultStates;
import io.jdbd.vendor.result.QuerySink;


public interface MultiResultsSink {

    void error(Throwable e);

    /**
     * @throws IllegalStateException when current result is query result.
     */
    void nextUpdate(ResultStates resultStates) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *                               <li>MultiResult complete.</li>
     *                               <li>current result is query result and not complete.</li>
     *                               </ul>
     */
    QuerySink nextQuery() throws IllegalStateException;

}
