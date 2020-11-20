package io.jdbd;

import org.reactivestreams.Publisher;

public interface DatabaseSessionFactory extends ReactiveCloseable{

    /**
     * @return {@link DatabaseSession} or {@link java.sql.SQLException}
     */
    Publisher<DatabaseSession> getSession();

}
