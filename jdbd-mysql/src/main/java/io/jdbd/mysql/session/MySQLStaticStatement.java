package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.statement.StaticStatement} with MySQL protocol.
 * </p>
 */
final class MySQLStaticStatement extends MySQLStatement<StaticStatement> implements StaticStatement {


    static MySQLStaticStatement create(final MySQLDatabaseSession session) {
        return new MySQLStaticStatement(session);
    }

    private MySQLStaticStatement(final MySQLDatabaseSession session) {
        super(session);
    }


    @Override
    public Publisher<ResultStates> executeUpdate(final String sql) {
        this.endStmtOption();
        final Mono<ResultStates> mono;
        if (MySQLStrings.hasText(sql)) {
            mono = this.session.protocol.update(Stmts.stmt(sql, this));
        } else {
            mono = Mono.error(MySQLExceptions.sqlIsEmpty());
        }
        return mono;
    }

    @Override
    public Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, CurrentRow::asResultRow, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function, Consumer<ResultStates> consumer) {
        this.endStmtOption();

        final Flux<R> flux;
        if (MySQLStrings.hasText(sql)) {
            flux = this.session.protocol.query(Stmts.stmt(sql, this), function, consumer);
        } else {
            flux = Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return flux;
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        this.endStmtOption();

        final Flux<ResultStates> flux;
        if (sqlGroup.size() == 0) {
            flux = Flux.error(MySQLExceptions.sqlIsEmpty());
        } else {
            flux = this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this));
        }
        return flux;
    }


    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        this.endStmtOption();

        final MultiResult result;
        if (sqlGroup.size() == 0) {
            result = MultiResults.error(MySQLExceptions.sqlIsEmpty());
        } else {
            result = this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this));
        }
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        this.endStmtOption();

        final OrderedFlux flux;
        if (sqlGroup.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        } else {
            flux = this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this));
        }
        return flux;
    }


    @Override
    public OrderedFlux executeAsFlux(final String multiStmt) {
        this.endStmtOption();

        final OrderedFlux flux;
        if (!MySQLStrings.hasText(multiStmt)) {
            flux = MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        } else if (!this.session.supportMultiStatement()) {
            flux = MultiResults.fluxError(MySQLExceptions.dontSupportMultiStmt());
        } else {
            flux = this.session.protocol.executeAsFlux(Stmts.multiStmt(multiStmt, this));
        }
        return flux;
    }


    /*################################## blow Statement method ##################################*/
    @Override
    public boolean supportPublisher() {
        // always false,MySQL COM_QUERY protocol don't support Publisher
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        // always false,MySQL COM_QUERY protocol don't support.
        return false;
    }


    /*################################## blow Statement packet template method ##################################*/

    @Override
    void checkReuse() {
        //no-op
    }

    /*################################## blow private static method ##################################*/


}
