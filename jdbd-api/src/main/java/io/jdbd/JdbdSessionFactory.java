package io.jdbd;

import org.reactivestreams.Publisher;

public interface JdbdSessionFactory extends ReactiveCloseable {

    /**
     * @return {@link TxDatabaseSession} or {@link java.sql.SQLException}
     */
    Publisher<TxDatabaseSession> getSession();

}
