package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.stmt.ParamSingleStmt;

interface PrepareStmtTask {

    ParamSingleStmt getStmt();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    int getStatementId();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    MySQLColumnMeta[] getParameterMetas();

    TaskAdjutant adjutant();

    int addAndGetSequenceId();

    default void startSafeSequenceId() {
        throw new UnsupportedOperationException();
    }

    default void endSafeSequenceId() {
        throw new UnsupportedOperationException();
    }

    default int safelyAddAndGetSequenceId() {
        throw new UnsupportedOperationException();
    }

    void addErrorToTask(Throwable error);

    boolean supportFetch();

    void nextGroupReset();

    void handleNoExecuteMessage();

    void handleLongParamSendFailure(Throwable error);

}
