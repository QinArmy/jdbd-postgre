package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.TransactionOption;
import reactor.core.publisher.Mono;

class MySQLSession extends AbstractStatelessSession implements DatabaseSession {

    MySQLSession() {
    }

    @Override
    public Mono<Void> startTransaction(TransactionOption option) {
        return null;
    }

    @Override
    public Mono<Void> commit() {
        return null;
    }

    @Override
    public Mono<Void> rollback() {
        return null;
    }

}
