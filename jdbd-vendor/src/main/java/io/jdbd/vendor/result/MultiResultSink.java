package io.jdbd.vendor.result;

import io.jdbd.result.ResultStates;


public interface MultiResultSink {

    /**
     * @throws IllegalStateException when {@link MultiResultSink} is terminated
     */
    void error(Throwable e) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *                               <li>{@link MultiResultSink} is terminated</li>
     *                               <li>current result is query result and not complete.</li>
     *                               </ul>
     */
    void nextUpdate(ResultStates resultStates) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *     <li>{@link MultiResultSink} is terminated</li>
     *     <li>current result is query result and not complete.</li>
     * </ul>
     */
    QuerySink nextQuery() throws IllegalStateException;


}
