package io.jdbd.postgre.protocol.client;

interface SimpleStmtTask {

    void addErrorToTask(Throwable error);

    TaskAdjutant adjutant();

    void handleNoQueryMessage();

}
