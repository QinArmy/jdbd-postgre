package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.DatabaseSessionFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.net.URI;

public class MySQLSessionFactory implements DatabaseSessionFactory {

    public static MySQLSessionFactory getInstance(String url, String user, String password) {
        return null;
    }

    MySQLSessionFactory(URI uri) {

    }

    @Override
    public Mono<DatabaseSession> getSession() {
        return null;
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

}
