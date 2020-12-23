package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;

import java.util.function.Consumer;

public interface CommTaskExecutorAdjutant {


    boolean inEventLoop();


    void syncSubmitTask(CommunicationTask<?> task, Consumer<Boolean> offerCall) throws JdbdNonSQLException;

    void execute(Runnable runnable);
}
