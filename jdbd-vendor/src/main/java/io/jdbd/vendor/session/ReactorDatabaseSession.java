package io.jdbd.vendor.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;
import java.util.List;

public interface ReactorDatabaseSession extends DatabaseSession {

    @Override
    StaticStatement statement();

    @Override
    Mono<PreparedStatement> prepare(String sql);

    @Override
    Mono<PreparedStatement> prepare(String sql, int executeTimeout);

    @Override
    BindableStatement bindable(String sql);

    @Override
    MultiStatement multi();

    @Override
    ReactorMultiResult multi(List<String> sqlList);

    @Override
    Mono<Savepoint> setSavepoint(String name);

    @Override
    Mono<Void> releaseSavePoint(Savepoint savepoint);

    @Override
    Mono<Void> rollbackToSavePoint(Savepoint savepoint);

    @Override
    Mono<Boolean> isClosed();


}
