package io.jdbd.postgre.session;

import io.jdbd.DatabaseSession;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.sql.Savepoint;

abstract class PgDatabaseSession implements DatabaseSession {

    final SessionAdjutant adjutant;

    final ClientProtocol protocol;

    PgDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        this.adjutant = adjutant;
        this.protocol = protocol;
    }

    @Override
    public final DatabaseMetaData getDatabaseMetaData() {
        return null;
    }

    @Override
    public final Mono<PreparedStatement> prepare(final String sql) {
        return this.protocol.prepare(sql, stmtTask -> PgPreparedStatement.create(this, stmtTask));
    }

    @Override
    public final StaticStatement statement() {
        return PgStaticStatement.create(this);
    }

    @Override
    public final BindStatement bindable(String sql) {
        return PgBindStatement.create(sql, this);
    }

    @Override
    public final MultiStatement multi() {
        return PgMultiStatement.create(this);
    }

    @Override
    public final boolean supportSavePoints() {
        return false;
    }

    @Override
    public final Publisher<Savepoint> setSavepoint() {
        return null;
    }

    @Override
    public final Publisher<Savepoint> setSavepoint(String name) {
        return null;
    }

    @Override
    public final Publisher<Void> releaseSavePoint(Savepoint savepoint) {
        return null;
    }

    @Override
    public final Publisher<Void> rollbackToSavePoint(Savepoint savepoint) {
        return null;
    }

    @Override
    public final Publisher<Boolean> isClosed() {
        return null;
    }

    @Override
    public final Publisher<Void> close() {
        return null;
    }


}
