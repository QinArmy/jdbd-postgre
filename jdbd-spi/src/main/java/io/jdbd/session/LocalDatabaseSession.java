package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;

import java.util.List;


public interface LocalDatabaseSession extends DatabaseSession {


    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option);

    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, HandleMode mode);

    boolean inTransaction() throws JdbdException;


    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    Publisher<LocalDatabaseSession> commit();

    Publisher<LocalDatabaseSession> commit(List<Option<?>> optionList);

    Publisher<LocalDatabaseSession> rollback();

    Publisher<LocalDatabaseSession> rollback(List<Option<?>> optionList);


}
