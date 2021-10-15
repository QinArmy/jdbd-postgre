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

    void startSafeSequenceId();

    void endSafeSequenceId();

    int safelyAddAndGetSequenceId();

    boolean supportFetch();

    void nextGroupReset();

}