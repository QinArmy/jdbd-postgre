package io.jdbd.session;

import org.reactivestreams.Publisher;

import java.sql.Connection;


public interface TxDatabaseSession extends DatabaseSession {

    /**
     * @see Connection#isReadOnly()
     * @see Connection#getTransactionIsolation()
     * @see Connection#getAutoCommit()
     */
    Publisher<TransactionOption> getTransactionOption();

    Publisher<TxDatabaseSession> startTransaction(TransactionOption option);

    Publisher<TxDatabaseSession> commit();

    Publisher<TxDatabaseSession> rollback();


}
