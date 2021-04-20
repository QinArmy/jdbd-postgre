package io.jdbd.vendor.result;

import io.jdbd.result.ResultStatus;


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
    void nextUpdate(ResultStatus resultStatus) throws IllegalStateException;

    /**
     * @throws IllegalStateException when <ul>
     *                               <li>{@link MultiResultSink} is terminated</li>
     *                               <li>current result is query result and not complete.</li>
     *                               </ul>
     */
    QuerySink nextQuery() throws IllegalStateException;

    /**
     * @throws IllegalStateException emit ,when last {@link ResultStatus#hasMoreResults()}  of update or query return true.
     */
    void complete();

}
