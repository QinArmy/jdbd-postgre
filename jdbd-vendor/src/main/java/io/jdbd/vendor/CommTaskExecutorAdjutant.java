package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;

public interface CommTaskExecutorAdjutant {


    boolean inEventLoop();


    boolean syncSubmitTask(CommunicationTask<?> task) throws JdbdNonSQLException;

    void execute(Runnable runnable);
}
