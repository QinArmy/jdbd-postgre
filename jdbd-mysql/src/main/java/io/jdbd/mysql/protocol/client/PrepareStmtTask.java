package io.jdbd.mysql.protocol.client;

interface PrepareStmtTask {

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    int obtainStatementId();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    MySQLColumnMeta[] obtainParameterMetas();

    TaskAdjutant obtainAdjutant();

    default void startSafeSequenceId() {
        throw new UnsupportedOperationException();
    }

    default void endSafeSequenceId() {
        throw new UnsupportedOperationException();
    }

    default int safelyAddAndGetSequenceId() {
        throw new UnsupportedOperationException();
    }

    boolean supportFetch();

    void nextGroupReset();

}
