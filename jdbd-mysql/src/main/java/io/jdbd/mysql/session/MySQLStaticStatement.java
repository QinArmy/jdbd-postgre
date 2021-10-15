package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.StaticStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;


/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.stmt.StaticStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLStaticStatement extends MySQLStatement implements StaticStatement {


    static MySQLStaticStatement create(final MySQLDatabaseSession session) {
        return new MySQLStaticStatement(session);
    }

    private int timeout = 0;

    private MySQLStaticStatement(MySQLDatabaseSession session) {
        super(session);
    }

    @Override
    public final boolean supportPublisher() {
        // always false,MySQL COM_QUERY protocol don't support Publisher
        return false;
    }

    @Override
    public final boolean supportOutParameter() {
        // always false,MySQL COM_QUERY protocol don't support.
        return false;
    }

    @Override
    public final Flux<ResultStates> executeBatch(final List<String> sqlGroup) {
        return this.session.protocol.batchUpdate(Stmts.stmts(sqlGroup, this.timeout));
    }

    @Override
    public final Mono<ResultStates> executeUpdate(String sql) {
        return this.session.protocol.update(Stmts.stmt(sql, this.timeout));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql) {
        return this.session.protocol.query(Stmts.stmt(sql, this.timeout));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql, Consumer<ResultStates> statesConsumer) {
        return this.session.protocol.query(Stmts.stmt(sql, statesConsumer, this.timeout));
    }

    @Override
    public final MultiResult executeAsMulti(final List<String> sqlGroup) {
        return this.session.protocol.executeAsMulti(Stmts.stmts(sqlGroup, this.timeout));
    }

    @Override
    public final OrderedFlux executeAsFlux(List<String> sqlGroup) {
        return null;
    }


    @Override
    public final OrderedFlux executeAsFlux(String multiStmt) {
        throw new UnsupportedOperationException();
    }


    /*################################## blow private static method ##################################*/


}
