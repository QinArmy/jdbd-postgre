package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.DatabaseSessionFactory;
import io.jdbd.ServerVersion;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.Savepoint;
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

    final ClientProtocol protocol;


    MySQLDatabaseSession(final ClientProtocol protocol) {
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
    public final boolean supportSavePoints() {
        return true;
    }

    @Override
    public final Mono<Savepoint> setSavepoint() {
        return this.protocol.setSavepoint();
    }

    @Override
    public final Mono<Savepoint> setSavepoint(final String name) {
        return this.protocol.setSavepoint();
    }

    @Override
    public final Mono<Void> releaseSavePoint(final Savepoint savepoint) {
        return this.protocol.releaseSavePoint(savepoint);
    }

    @Override
    public final Mono<Void> rollbackToSavePoint(final Savepoint savepoint) {
        return this.protocol.rollbackToSavePoint(savepoint);
    }

    @Override
    public final Mono<Boolean> isClosed() {
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
        return this.protocol.closeGracefully();
    }



    /*################################## blow private method ##################################*/

    private PreparedStatement createPreparedStatement(final PrepareTask<MySQLType> task) {
        return MySQLPreparedStatement.create(this, task);
    }


}
