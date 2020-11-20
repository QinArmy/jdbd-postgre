package io.jdbd;

import org.reactivestreams.Publisher;


public interface DatabaseSession extends StatelessSession {


    Publisher<Void> startTransaction(TransactionOption option);

    Publisher<Void> commit();

    Publisher<Void> rollback();

}
