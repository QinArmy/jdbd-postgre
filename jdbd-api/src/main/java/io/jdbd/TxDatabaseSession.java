package io.jdbd;

import org.reactivestreams.Publisher;


public interface TxDatabaseSession extends DatabaseSession {


    Publisher<Void> startTransaction(TransactionOption option);

    Publisher<Void> commit();

    Publisher<Void> rollback();





}
