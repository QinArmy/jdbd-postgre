package io.jdbd;

import reactor.core.publisher.Mono;

public interface DatabaseSessionFactory extends ReactiveCloseable{

    /**
     * @return {@link DatabaseSession} or {@link java.sql.SQLException}
     */
    Mono<DatabaseSession> getSession();

}
