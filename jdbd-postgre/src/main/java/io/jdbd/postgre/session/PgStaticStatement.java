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

    private boolean executed;

    private PgStaticStatement(PgDatabaseSession<?> session) {
        super(session);
    }

    @Override
    public Publisher<ResultStates> executeUpdate(final String sql) {
        final Mono<ResultStates> mono;
        if (this.executed) {
            mono = Mono.error(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgStrings.hasText(sql)) {
            mono = this.session.protocol.update(Stmts.stmt(sql, this));
        } else {
            mono = Mono.error(PgExceptions.sqlHaveNoText());
        }
        this.executed = true;
        return mono;
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
        if (this.executed) {
            flux = Flux.error(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (!PgStrings.hasText(sql)) {
            flux = Flux.error(PgExceptions.sqlHaveNoText());
        } else if (function == null) {
            flux = Flux.error(PgExceptions.queryMapFuncIsNull());
        } else if (consumer == null) {
            flux = Flux.error(PgExceptions.statesConsumerIsNull());
        } else {
            flux = this.session.protocol.query(Stmts.stmt(sql, consumer, this), function);
        }
        this.executed = true;
        return flux;
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        final Flux<ResultStates> flux;
        if (this.executed) {
            flux = Flux.error(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgCollections.isEmpty(sqlGroup)) {
            flux = Flux.error(PgExceptions.sqlHaveNoText());
        } else {
            flux = this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this));
        }
        this.executed = true;
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery(final List<String> sqlGroup) {

        final BatchQuery batchQuery;
        if (this.executed) {
            batchQuery = MultiResults.batchQueryError(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgCollections.isEmpty(sqlGroup)) {
            batchQuery = MultiResults.batchQueryError(PgExceptions.sqlHaveNoText());
        } else {
            batchQuery = this.session.protocol.batchQuery(Stmts.batch(sqlGroup, this));
        }
        this.executed = true;
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        final MultiResult multiResult;
        if (this.executed) {
            multiResult = MultiResults.multiError(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgCollections.isEmpty(sqlGroup)) {
            multiResult = MultiResults.multiError(PgExceptions.sqlHaveNoText());
        } else {
            multiResult = this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this));
        }
        this.executed = true;
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        final OrderedFlux flux;
        if (this.executed) {
            flux = MultiResults.fluxError(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgCollections.isEmpty(sqlGroup)) {
            flux = MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        } else {
            flux = this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this));
        }
        this.executed = true;
        return flux;
    }

    @Override
    public OrderedFlux executeAsFlux(final String multiStmt) {
        final OrderedFlux flux;
        if (this.executed) {
            flux = MultiResults.fluxError(PgExceptions.cannotReuseStatement(StaticStatement.class));
        } else if (PgStrings.hasText(multiStmt)) {
            flux = this.session.protocol.executeAsFlux(Stmts.multiStmt(multiStmt, this));
        } else {
            flux = MultiResults.fluxError(PgExceptions.sqlHaveNoText());
        }
        this.executed = true;
        return flux;
    }


    @Override
    public String toString() {
        return PgStrings.builder(40)
                .append(getClass().getName())
                .append("[ factory : ")
                .append(this.session.factoryName())
                .append(" , session : ")
                .append(this.session.sessionIdentifier())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
