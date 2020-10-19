package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.DatabaseSessionFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

abstract class AbstractMySQLSessionFactory implements DatabaseSessionFactory {




    @Override
    public final Mono<DatabaseSession> getSession() {
        return null;
    }


    private Mono<Connection> getConnection() {
            return Mono.empty();
    }

}
