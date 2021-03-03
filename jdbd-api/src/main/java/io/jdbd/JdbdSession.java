package io.jdbd;

import org.reactivestreams.Publisher;


public interface JdbdSession extends StatelessSession {


    Publisher<Void> startTransaction(TransactionOption option);

    Publisher<Void> commit();

    Publisher<Void> rollback();


    StaticStatement staticStmt();

    BindableStatement bindableStmt();


}
