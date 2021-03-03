package io.jdbd.mysql;

import io.jdbd.PreparedStatement;
import io.jdbd.StatelessSession;
import io.jdbd.StaticStatement;
import io.jdbd.TransactionOption;
import io.jdbd.meta.DatabaseMetaData;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;

abstract class AbstractStatelessSession implements StatelessSession {

     AbstractStatelessSession() {
    }

    @Override
    public DatabaseMetaData getDatabaseMetaData() {
        return null;
    }

    @Override
    public Mono<? extends TransactionOption> getTransactionOption() {
        return null;
    }

    @Override
    public Mono<StaticStatement> createStatement() {
        return null;
    }

    @Override
    public Mono<PreparedStatement> prepareStatement(String sql) {
        return null;
    }

    @Override
    public boolean supportSavePoints() {
        return false;
    }

    @Override
    public Mono<? extends Savepoint> setSavepoint() {
        return null;
    }

    @Override
    public Mono<? extends Savepoint> setSavepoint(String name) {
        return null;
    }

    @Override
    public Mono<Void> releaseSavePoint(Savepoint savepoint) {
        return null;
    }

    @Override
    public Mono<Void> rollbackToSavePoint(Savepoint savepoint) {
        return null;
    }

    @Override
    public Mono<Void> close() {
        return null;
    }

    @Override
    public Mono<Boolean> isClosed() {
        return null;
    }
}
