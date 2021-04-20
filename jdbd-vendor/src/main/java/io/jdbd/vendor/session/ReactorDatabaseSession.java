package io.jdbd.vendor.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.ReactorBindableStatement;
import io.jdbd.vendor.stmt.ReactorMultiStatement;
import io.jdbd.vendor.stmt.ReactorStaticStatement;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;
import java.util.List;

public interface ReactorDatabaseSession extends DatabaseSession {

    @Override
    ReactorStaticStatement statement();

    @Override
    Mono<PreparedStatement> prepare(String sql);

    @Override
    Mono<PreparedStatement> prepare(String sql, int executeTimeout);

    @Override
    ReactorBindableStatement bindable(String sql);

    @Override
    ReactorMultiStatement multi();

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
