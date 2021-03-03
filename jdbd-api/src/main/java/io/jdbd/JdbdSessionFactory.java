package io.jdbd;

import org.reactivestreams.Publisher;

public interface JdbdSessionFactory extends ReactiveCloseable {

    /**
     * @return {@link JdbdSession} or {@link java.sql.SQLException}
     */
    Publisher<JdbdSession> getSession();

}
