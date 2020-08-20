package io.jdbd;

import reactor.core.publisher.Mono;

public interface DatabaseSessionFactory {

    /**
     * @return {@link DatabaseSession} or {@link java.sql.SQLException}
     */
    Mono<? extends DatabaseSession> getSession();

}
