package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.TransactionOption;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.result.MultiResult;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;
import java.util.List;

abstract class AbstractStatelessSession implements DatabaseSession {

    AbstractStatelessSession() {
    }

    @Override
    public DatabaseMetaData getDatabaseMetaData() {
        return null;
    }

    @Override
    public Mono<TransactionOption> getTransactionOption() {
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
    public Publisher<PreparedStatement> prepare(String sql, int executeTimeout) {
        return null;
    }

    @Override
    public BindableStatement bindable(String sql) {
        return null;
    }

    @Override
    public MultiStatement multi() {
        return null;
    }

    @Override
    public MultiResult multi(List<String> sqlList) {
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
