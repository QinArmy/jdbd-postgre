package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.util.JdbdFunctions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.statement.StaticStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLStaticStatement extends MySQLStatement implements StaticStatement {


    static MySQLStaticStatement create(final MySQLDatabaseSession session) {
        return new MySQLStaticStatement(session);
    }

    private MySQLStaticStatement(final MySQLDatabaseSession session) {
        super(session);
    }


    @Override
    public Mono<ResultStates> executeUpdate(final String sql) {
        final Mono<ResultStates> mono;
        if (MySQLStrings.hasText(sql)) {
            mono = this.session.protocol.update(Stmts.stmt(sql, this.statementOption));
        } else {
            mono = Mono.error(MySQLExceptions.createEmptySqlException());
        }
        return mono;
    }

    @Override
    public Flux<ResultRow> executeQuery(final String sql) {
        return executeQuery(sql, JdbdFunctions.noActionConsumer());
    }

    @Override
    public Flux<ResultRow> executeQuery(final String sql, final Consumer<ResultStates> statesConsumer) {
        Objects.requireNonNull(statesConsumer, "statesConsumer");
        final Flux<ResultRow> flux;
        if (MySQLStrings.hasText(sql)) {
            flux = this.session.protocol.query(Stmts.stmt(sql, statesConsumer, this.statementOption));
        } else {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        Objects.requireNonNull(sqlGroup, "sqlGroup");
        final Flux<ResultStates> flux;
        if (sqlGroup.size() == 0) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this.statementOption));
        }
        return flux;
    }


    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        Objects.requireNonNull(sqlGroup, "sqlGroup");
        final MultiResult result;
        if (sqlGroup.size() == 0) {
            result = MultiResults.error(MySQLExceptions.createEmptySqlException());
        } else {
            result = this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this.statementOption));
        }
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        Objects.requireNonNull(sqlGroup, "sqlGroup");
        final OrderedFlux flux;
        if (sqlGroup.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.createEmptySqlException());
        } else {
            flux = this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this.statementOption));
        }
        return flux;
    }


    @Override
    public OrderedFlux executeAsFlux(final String multiStmt) {
        final OrderedFlux flux;
        if (!MySQLStrings.hasText(multiStmt)) {
            flux = MultiResults.fluxError(MySQLExceptions.createEmptySqlException());
        } else if (!this.session.supportMultiStatement()) {
            flux = MultiResults.fluxError(MySQLExceptions.createMultiStatementException());
        } else {
            flux = this.session.protocol.executeAsFlux(Stmts.multiStmt(multiStmt, this.statementOption));
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

    @Override
    public boolean setFetchSize(int fetchSize) {
        // always false,MySQL ComQuery protocol don't support.
        return false;
    }

    /*################################## blow Statement packet template method ##################################*/

    @Override
    void checkReuse() {
        //no-op
    }

    /*################################## blow private static method ##################################*/


}
