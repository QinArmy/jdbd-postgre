package io.jdbd;

public interface Statement {

    /**
     * Sets the number of seconds the driver will wait for a
     * <code>Statement</code> object to execute to the given number of seconds.
     * By default there is no limit on the amount of time allowed for a running
     * statement to complete. If the limit is exceeded, an
     * <code>SQLTimeoutException</code> is thrown.
     * A JDBC driver must apply this limit to the <code>execute</code>,
     * <code>executeQuery</code> and <code>executeUpdate</code> methods.
     * <p>
     * <strong>Note:</strong> JDBC driver implementations may also apply this
     * limit to {@code ResultSet} methods
     * (consult your driver vendor documentation for details).
     * <p>
     * <strong>Note:</strong> In the case of {@code Statement} batching, it is
     * implementation defined as to whether the time-out is applied to
     * individual SQL commands added via the {@code addBatch} method or to
     * the entire batch of SQL commands invoked by the {@code executeBatch}
     * method (consult your driver vendor documentation for details).
     *
     * @param seconds the new query timeout limit in seconds; zero means
     *                there is no limit
     *                this method is called on a closed <code>Statement</code>
     *                or the condition {@code seconds >= 0} is not satisfied
     * @see #getQueryTimeout
     */
    Statement setExecuteTimeout(int seconds);


    boolean supportLongData();
}
