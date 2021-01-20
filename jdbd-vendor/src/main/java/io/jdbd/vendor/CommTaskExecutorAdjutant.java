package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;

import java.util.function.Consumer;

public interface CommTaskExecutorAdjutant {


    boolean inEventLoop();

    void syncSubmitTask(CommunicationTask<?> task, Consumer<Boolean> offerCall) throws JdbdNonSQLException;

    void execute(Runnable runnable);

    /**
     * @throws JdbdNonSQLException when current thread not in EventLoop.
     */
    boolean isAutoCommit() throws JdbdNonSQLException;

    @Nullable
    Object getServerStatus() throws JdbdNonSQLException;
}
