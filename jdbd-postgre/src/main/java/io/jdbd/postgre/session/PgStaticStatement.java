package io.jdbd.postgre.session;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This class is a implementation of {@link StaticStatement} with postgre client protocol.
 * </p>
 *
 * @since 1.0
 */
final class PgStaticStatement extends PgStatement<StaticStatement> implements StaticStatement {

    static PgStaticStatement create(PgDatabaseSession<?> session) {
        return new PgStaticStatement(session);
    }

    private PgStaticStatement(PgDatabaseSession<?> session) {
        super(session);
    }

    @Override
    public Publisher<ResultStates> executeUpdate(final String sql) {
        if (!PgStrings.hasText(sql)) {
            return Mono.error(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.update(Stmts.stmt(sql, this));
    }

    @Override
    public Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, DatabaseProtocol.ROW_FUNC, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, @Nullable Function<CurrentRow, R> function,
                                         @Nullable Consumer<ResultStates> consumer) {
        final Flux<R> flux;
        if (!PgStrings.hasText(sql)) {
            flux = Flux.error(PgExceptions.sqlHaveNoText());
        } else if (function == null) {
            flux = Flux.error(PgExceptions.queryMapFuncIsNull());
        } else if (consumer == null) {
            flux = Flux.error(PgExceptions.statesConsumerIsNull());
        } else {
            flux = this.session.protocol.query(Stmts.stmt(sql, consumer, this), function);
        }
        return flux;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
     */
    @Override
    public Publisher<RefCursor> declareCursor(final String sql) {
        if (!PgStrings.hasText(sql)) {
            return Mono.error(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.declareCursor(Stmts.stmt(sql, this));
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return Flux.error(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this));
    }

    @Override
    public BatchQuery executeBatchQuery(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.batchQuery(Stmts.batch(sqlGroup, this));
    }

    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.error(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this));
    }

    @Override
    public OrderedFlux executeBatchAsFlux(List<String> sqlGroup) {
        if (PgCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this));
    }

    @Override
    public OrderedFlux executeAsFlux(final String multiStmt) {
        if (!PgStrings.hasText(multiStmt)) {
            return MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        }
        return this.session.protocol.executeAsFlux(Stmts.multiStmt(multiStmt, this));
    }


    @Override
    public String toString() {
        return PgStrings.builder(40)
                .append(getClass().getName())
                .append("[ factory : ")
                .append(this.session.factoryName())
                .append(" , session : ")
                .append(this.session.identifier())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
