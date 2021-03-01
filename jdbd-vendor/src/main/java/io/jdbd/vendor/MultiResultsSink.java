package io.jdbd.vendor;

import io.jdbd.ResultStates;
import io.jdbd.vendor.result.QuerySink;


public interface MultiResultsSink {

    /**
     * @throws IllegalStateException when {@link MultiResultsSink} is terminated
     */
    void error(Throwable e) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *                               <li>{@link MultiResultsSink} is terminated</li>
     *                               <li>current result is query result and not complete.</li>
     *                               </ul>
     */
    void nextUpdate(ResultStates resultStates) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *     <li>{@link MultiResultsSink} is terminated</li>
     *     <li>current result is query result and not complete.</li>
     * </ul>
     */
    QuerySink nextQuery() throws IllegalStateException;

}
