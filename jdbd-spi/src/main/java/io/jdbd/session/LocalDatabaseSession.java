package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface LocalDatabaseSession extends DatabaseSession {


    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option);

    default boolean inTransaction() {
        return false;
    }


    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    Publisher<LocalDatabaseSession> commit();

    Publisher<LocalDatabaseSession> rollback();


}
