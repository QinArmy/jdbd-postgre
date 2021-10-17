package io.jdbd.mysql.session;

import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.SavePoint;
import io.jdbd.session.ServerVersion;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Mono;


/**
 * <p>
 * This class is a implementation of {@link DatabaseSession} with MySQL protocol.
 * This class is base class of below class:
 * <ul>
 *     <li>{@link MySQLStaticStatement}</li>
 *     <li>{@link MySQLPreparedStatement}</li>
 *     <li>{@link MySQLBindStatement}</li>
 *     <li>{@link MySQLMultiStatement}</li>
 * </ul>
 *
 * </p>
 */
abstract class MySQLDatabaseSession implements DatabaseSession {

    final SessionAdjutant adjutant;

    final ClientProtocol protocol;

    MySQLDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        this.adjutant = adjutant;
        this.protocol = protocol;
    }

    @Override
    public final StaticStatement statement() {
        return MySQLStaticStatement.create(this);
    }

    @Override
    public final Mono<PreparedStatement> prepare(final String sql) {
        if (!MySQLStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must has text.");
        }
        return this.protocol.prepare(sql, this::createPreparedStatement);
    }


    @Override
    public final BindStatement bindable(final String sql) {
        if (!MySQLStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must has text.");
        }
        return MySQLBindStatement.create(this, sql);
    }

    @Override
    public final MultiStatement multi() {
        return MySQLMultiStatement.create(this);
    }

    @Override
    public final DatabaseMetaData getDatabaseMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean supportMultiStatement() {
        return this.protocol.supportMultiStmt();
    }

    @Override
    public final boolean supportSavePoints() {
        return true;
    }

    @Override
    public final Mono<SavePoint> setSavePoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Mono<SavePoint> setSavePoint(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Mono<Void> releaseSavePoint(final SavePoint savepoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Mono<Void> rollbackToSavePoint(final SavePoint savepoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isClosed() {
        return this.protocol.isClosed();
    }

    @Override
    public final ServerVersion getServerVersion() {
        return this.protocol.getServerVersion();
    }

    @Override
    public final boolean isSameFactory(DatabaseSession session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isBelongTo(DatabaseSessionFactory factory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Mono<Void> close() {
        return this.protocol.close();
    }



    /*################################## blow private method ##################################*/

    private PreparedStatement createPreparedStatement(final PrepareTask<MySQLType> task) {
        return MySQLPreparedStatement.create(this, task);
    }


}
