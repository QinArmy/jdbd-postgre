package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;

abstract class AbstractStatelessSession implements DatabaseSession {

    AbstractStatelessSession() {
    }

    @Override
    public DatabaseMetaData getDatabaseMetaData() {
        return null;
    }


    @Override
    public StaticStatement statement() {
        return null;
    }

    @Override
    public Mono<PreparedStatement> prepare(String sql) {
        return null;
    }


    @Override
    public BindStatement bindable(String sql) {
        return null;
    }

    @Override
    public MultiStatement multi() {
        return null;
    }

    @Override
    public boolean supportSavePoints() {
        return false;
    }

    @Override
    public Mono<Savepoint> setSavepoint() {
        return null;
    }

    @Override
    public Mono<Savepoint> setSavepoint(String name) {
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
