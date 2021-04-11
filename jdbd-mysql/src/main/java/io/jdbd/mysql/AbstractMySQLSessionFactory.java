package io.jdbd.mysql;

import io.jdbd.JdbdSessionFactory;
import io.jdbd.TxDatabaseSession;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

abstract class AbstractMySQLSessionFactory implements JdbdSessionFactory {


    @Override
    public final Mono<TxDatabaseSession> getSession() {
        return null;
    }


    private Mono<Connection> getConnection() {
            return Mono.empty();
    }

}
