package io.jdbd.vendor.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;

public interface ReactorDatabaseSession extends DatabaseSession {

    @Override
    StaticStatement statement();

    @Override
    Mono<PreparedStatement> prepare(String sql);

    @Override
    BindStatement bindable(String sql);

    @Override
    MultiStatement multi();


    @Override
    Mono<Savepoint> setSavePoint(String name);

    @Override
    Mono<Void> releaseSavePoint(Savepoint savepoint);

    @Override
    Mono<Void> rollbackToSavePoint(Savepoint savepoint);

    @Override
    Mono<Boolean> isClosed();


}
