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

   void resetSequenceId();

    void addErrorToTask(Throwable error);

    boolean supportFetch();

    void nextGroupReset();

    void handleExecuteMessageError();


}
