package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface TxDatabaseSession extends DatabaseSession {


    Publisher<TxDatabaseSession> setTransactionOption(TransactionOption option);

    Publisher<TxDatabaseSession> startTransaction(TransactionOption option);


    Publisher<TxDatabaseSession> commit();

    Publisher<TxDatabaseSession> rollback();


}
