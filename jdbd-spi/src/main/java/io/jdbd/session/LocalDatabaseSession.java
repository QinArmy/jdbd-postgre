package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;


public interface LocalDatabaseSession extends DatabaseSession {


    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option);

    boolean inTransaction() throws JdbdException;


    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    Publisher<LocalDatabaseSession> commit();

    Publisher<LocalDatabaseSession> rollback();


}
