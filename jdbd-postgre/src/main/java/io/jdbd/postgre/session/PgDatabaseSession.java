package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.session.SavePoint;
import io.jdbd.session.TransactionOption;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.NamedSavePoint;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-savepoint.html">SAVEPOINT</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-release-savepoint.html">RELEASE SAVEPOINT</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-rollback-to.html">ROLLBACK TO SAVEPOINT</a>
 * @since 1.0
 */
abstract class PgDatabaseSession<S extends DatabaseSession> extends PgDatabaseMetaSpec implements DatabaseSession {

    final PgDatabaseSessionFactory factory;

    final Function<String, DataType> internalOrUserTypeFunc;

    private final AtomicInteger savePointIndex = new AtomicInteger(0);

    PgDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(protocol);
        this.factory = factory;
        this.internalOrUserTypeFunc = protocol.internalOrUserTypeFunc();
    }

    @Override
    public final String factoryName() {
        return this.factory.name();
    }

    @Override
    public final long sessionIdentifier() throws JdbdException {
        return this.protocol.sessionIdentifier();
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
                                               @Nullable Consumer<ResultStates> consumer) {
        final Flux<R> flux;
        if (!PgStrings.hasText(sql)) {
            flux = Flux.error(PgExceptions.sqlHaveNoText());
        } else if (function == null) {
            flux = Flux.error(PgExceptions.queryMapFuncIsNull());
        } else if (consumer == null) {
            flux = Flux.error(PgExceptions.statesConsumerIsNull());
        } else {
            flux = this.protocol.query(Stmts.stmt(sql, consumer), function);
        }
        return flux;
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
    public final DatabaseMetaData databaseMetaData() {
        if (this.protocol.isClosed()) {
            throw PgExceptions.sessionHaveClosed();
        }
        return PgDatabaseMetaData.create(this.protocol);
    }

    @Override
    public final StaticStatement statement() {
        return PgStaticStatement.create(this);
    }

    @Override
    public final Publisher<PreparedStatement> prepareStatement(final String sql) {
        if (!PgStrings.hasText(sql)) {
            return Mono.error(PgExceptions.sqlHaveNoText());
        }
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
    public final RefCursor refCursor(String name) {
        return this.protocol.refCursor(name, Collections.emptyMap(), this);
    }

    @Override
    public final RefCursor refCursor(String name, Map<Option<?>, ?> optionMap) {
        return this.protocol.refCursor(name, optionMap, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> setTransactionCharacteristics(TransactionOption option) {
        return this.protocol.setTransactionCharacteristics(option)
                .thenReturn((S) this);
    }

    @Override
    public final boolean inTransaction() throws JdbdException {
        return this.protocol.inTransaction();
    }

    @Override
    public final Publisher<SavePoint> setSavePoint() {
        final StringBuilder builder;
        builder = PgStrings.builder()
                .append("jdbd_")
                .append(this.savePointIndex.getAndIncrement())
                .append('_')
                .append(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        return this.setSavePoint(builder.toString(), Collections.emptyMap());
    }

    @Override
    public final Publisher<SavePoint> setSavePoint(String name) {
        return this.setSavePoint(name, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<SavePoint> setSavePoint(final String name, final Map<Option<?>, ?> optionMap) {
        final Mono<SavePoint> mono;
        if (!PgStrings.hasText(name)) {
            mono = Mono.error(PgExceptions.savePointNameIsEmpty());
        } else if (!PgCollections.isEmpty(optionMap)) {
            mono = Mono.error(PgExceptions.dontSupportOptionMap(PgDriver.POSTGRE_SQL, "setSavePoint", optionMap));
        } else {
            final StringBuilder builder = new StringBuilder(25);
            builder.append("SAVEPOINT ");
            this.protocol.bindIdentifier(builder, name);
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .thenReturn(NamedSavePoint.fromName(name));
        }
        return mono;
    }


    @Override
    public final Publisher<S> releaseSavePoint(final SavePoint savepoint) {
        return this.releaseSavePoint(savepoint, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-release-savepoint.html">RELEASE SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> releaseSavePoint(final SavePoint savepoint, final Map<Option<?>, ?> optionMap) {
        final String name;
        final Mono<S> mono;
        if (!(savepoint instanceof NamedSavePoint && PgStrings.hasText((name = savepoint.name())))) {
            mono = Mono.error(PgExceptions.unknownSavePoint(savepoint));
        } else if (!PgCollections.isEmpty(optionMap)) {
            mono = Mono.error(PgExceptions.dontSupportOptionMap(PgDriver.POSTGRE_SQL, "releaseSavePoint", optionMap));
        } else {
            final StringBuilder builder = new StringBuilder(30);
            builder.append("RELEASE SAVEPOINT ");
            this.protocol.bindIdentifier(builder, name);
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .thenReturn((S) this);
        }
        return mono;
    }

    @Override
    public final Publisher<S> rollbackToSavePoint(final SavePoint savepoint) {
        return this.rollbackToSavePoint(savepoint, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-rollback-to.html">ROLLBACK TO SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> rollbackToSavePoint(final SavePoint savepoint, final Map<Option<?>, ?> optionMap) {
        final String name;
        final Mono<S> mono;
        if (!(savepoint instanceof NamedSavePoint && PgStrings.hasText((name = savepoint.name())))) {
            mono = Mono.error(PgExceptions.unknownSavePoint(savepoint));
        } else if (!PgCollections.isEmpty(optionMap)) {
            mono = Mono.error(PgExceptions.dontSupportOptionMap(PgDriver.POSTGRE_SQL, "rollbackToSavePoint", optionMap));
        } else {
            final StringBuilder builder = new StringBuilder(30);
            builder.append("ROLLBACK TO SAVEPOINT ");
            this.protocol.bindIdentifier(builder, name);
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .thenReturn((S) this);
        }
        return mono;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS">Identifiers and Key Words</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S bindIdentifier(StringBuilder builder, String identifier) {
        this.protocol.bindIdentifier(builder, identifier);
        return (S) this;
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
