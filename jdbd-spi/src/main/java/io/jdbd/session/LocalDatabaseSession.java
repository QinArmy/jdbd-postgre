package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This interface representing database session that support local transaction.
 * </p>
 *
 * @since 1.0
 */
public interface LocalDatabaseSession extends DatabaseSession {


    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option);

    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, HandleMode mode);

    Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, Map<Option<?>, ?> optionMap, HandleMode mode);

    boolean inTransaction() throws JdbdException;


    @Override
    Publisher<LocalDatabaseSession> releaseSavePoint(SavePoint savepoint);

    @Override
    Publisher<LocalDatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    Publisher<LocalDatabaseSession> commit();

    Publisher<LocalDatabaseSession> commit(Map<Option<?>, ?> optionMap);

    Publisher<LocalDatabaseSession> rollback();

    Publisher<LocalDatabaseSession> rollback(Map<Option<?>, ?> optionMap);


    @Override
    LocalDatabaseSession bindIdentifier(StringBuilder builder, String identifier);


}
