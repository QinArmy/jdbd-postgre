package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SavePoint;
import io.jdbd.session.TransactionStatus;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * This class is a implementation of {@link DatabaseSession} with postgre client protocol.
 * <p>
 * This class is base class of :
 *     <ul>
 *         <li>{@link PgLocalDatabaseSession}</li>
 *         <li>{@link PgRmDatabaseSession}</li>
 *     </ul>
 * </p>
 */
abstract class PgDatabaseSession<S extends DatabaseSession> extends PgDatabaseMetaSpec implements DatabaseSession {

    final PgDatabaseSessionFactory factory;

    PgDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(protocol);
        this.factory = factory;
    }

    @Override
    public final String factoryName() {
        return this.factory.name();
    }

    @Override
    public final long identifier() throws JdbdException {
        return this.protocol.identifier();
    }

    @Override
    public final Publisher<ResultStates> executeUpdate(final String sql) {
        if (!PgStrings.hasText(sql)) {
            return Mono.error(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.update(Stmts.stmt(sql));
    }

    @Override
    public final Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, DatabaseProtocol.ROW_FUNC, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, @Nullable Function<CurrentRow, R> function,
                                               @Nullable Consumer<ResultStates> statesConsumer) {
        final Flux<R> flux;
        if (!PgStrings.hasText(sql)) {
            flux = Flux.error(PgExceptions.sqlHaveNoText());
        } else if (function == null) {
            flux = Flux.error(PgExceptions.queryMapFuncIsNull());
        } else if (statesConsumer == null) {
            flux = Flux.error(PgExceptions.statesConsumerIsNull());
        } else {
            flux = this.protocol.query(Stmts.stmt(sql, statesConsumer), function);
        }
        return flux;
    }

    @Override
    public final Publisher<RefCursor> declareCursor(final String sql) {
        if (!PgStrings.hasText(sql)) {
            return Mono.error(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.declareCursor(Stmts.stmt(sql));
    }

    @Override
    public final Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return Flux.error(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.batchUpdate(Stmts.batch(sqlGroup));
    }

    @Override
    public final BatchQuery executeBatchQuery(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.batchQuery(Stmts.batch(sqlGroup));
    }

    @Override
    public final MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.error(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.batchAsMulti(Stmts.batch(sqlGroup));
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.batchAsFlux(Stmts.batch(sqlGroup));
    }

    @Override
    public final OrderedFlux executeAsFlux(final String multiStmt) {
        if (!PgStrings.hasText(multiStmt)) {
            return MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        }
        return this.protocol.executeAsFlux(Stmts.multiStmt(multiStmt));
    }

    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        return this.protocol.transactionStatus();
    }

    @Override
    public final DatabaseMetaData databaseMetaData() {
        return PgDatabaseMetaData.create(this.protocol);
    }

    @Override
    public final StaticStatement statement() {
        return PgStaticStatement.create(this);
    }

    @Override
    public final Mono<PreparedStatement> prepare(final String sql) {
        return this.protocol.prepare(sql)
                .map(this::createPreparedStatement);
    }

    @Override
    public final BindStatement bindStatement(final String sql) {
        return this.bindStatement(sql, false);
    }


    @Override
    public final BindStatement bindStatement(String sql, boolean forceServerPrepared) {
        if (!PgStrings.hasText(sql)) {
            throw PgExceptions.bindSqlHaveNoText();
        }
        return PgBindStatement.create(sql, this, forceServerPrepared);
    }

    @Override
    public final MultiStatement multiStatement() {
        return PgMultiStatement.create(this);
    }

    @Override
    public final Publisher<SavePoint> setSavePoint() {
        return null;
    }

    @Override
    public final Publisher<SavePoint> setSavePoint(String name) {
        return null;
    }

    @Override
    public final Publisher<S> releaseSavePoint(SavePoint savepoint) {
        return null;
    }

    @Override
    public final Publisher<S> rollbackToSavePoint(SavePoint savepoint) {
        return null;
    }


    @Override
    public final boolean isClosed() {
        return this.protocol.isClosed();
    }

    @Override
    public final <T> Publisher<T> close() {
        return this.protocol.close();
    }

    @Override
    public final boolean isSameFactory(DatabaseSession session) {
        return session instanceof PgDatabaseSession
                && ((PgDatabaseSession<?>) session).factory == this.factory;
    }



    /*################################## blow private method ##################################*/

    private PgPreparedStatement createPreparedStatement(final PrepareTask stmtTask) {
        return PgPreparedStatement.create(this, stmtTask);
    }


}
