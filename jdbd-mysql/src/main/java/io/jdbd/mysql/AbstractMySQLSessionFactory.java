package io.jdbd.mysql;

import io.jdbd.JdbdSession;
import io.jdbd.JdbdSessionFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

abstract class AbstractMySQLSessionFactory implements JdbdSessionFactory {


    @Override
    public final Mono<JdbdSession> getSession() {
        return null;
    }


    private Mono<Connection> getConnection() {
            return Mono.empty();
    }

}
