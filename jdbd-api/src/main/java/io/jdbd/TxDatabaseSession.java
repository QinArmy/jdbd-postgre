package io.jdbd;

import org.reactivestreams.Publisher;

import java.sql.Connection;


public interface TxDatabaseSession extends DatabaseSession {

    /**
     * @see Connection#isReadOnly()
     * @see Connection#getTransactionIsolation()
     * @see Connection#getAutoCommit()
     */
    Publisher<TransactionOption> getTransactionOption();

    Publisher<Void> startTransaction(TransactionOption option);

    Publisher<Void> commit();

    Publisher<Void> rollback();





}
