package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.ReactorStaticStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;


/**
 * <p>
 * This class is a implementation of {@link StaticStatement}.
 * </p>
 *
 * @param <S> databaseSession type
 */
final class MySQLStaticStatement<S extends MySQLDatabaseSession> extends MySQLStatement<S>
        implements ReactorStaticStatement {


    static <S extends MySQLDatabaseSession> MySQLStaticStatement<S> create(S session) {
        return new MySQLStaticStatement<>(session);
    }

    private int timeout = 0;

    private MySQLStaticStatement(S session) {
        super(session);
    }

    @Override
    public final boolean supportLongData() {
        // always false,MySQL COM_QUERY protocol don't support Publisher
        return false;
    }

    @Override
    public final boolean supportOutParameter() {
        // always false,MySQL COM_QUERY protocol don't support.
        return false;
    }

    @Override
    public final Flux<ResultStatus> executeBatch(final List<String> sqlList) {
        return this.session.protocol.batchUpdate(Stmts.stmts(sqlList, this.timeout));
    }

    @Override
    public final Mono<ResultStatus> executeUpdate(String sql) {
        return this.session.protocol.update(Stmts.stmt(sql, this.timeout));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql) {
        return this.session.protocol.query(Stmts.stmt(sql, this.timeout));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql, Consumer<ResultStatus> statesConsumer) {
        return this.session.protocol.query(Stmts.stmt(sql, statesConsumer, this.timeout));
    }

    @Override
    public final ReactorMultiResult executeAsMulti(final List<String> sqlList) {
        return this.session.protocol.executeAsMulti(Stmts.stmts(sqlList, this.timeout));
    }

    @Override
    public final Flux<SingleResult> executeAsFlux(List<String> sqlList) {
        return this.session.protocol.executeAsFlux(Stmts.stmts(sqlList, this.timeout));
    }

    @Override
    public final boolean setExecuteTimeout(int seconds) {
        this.timeout = seconds;
        return seconds > 0;
    }

    /*################################## blow private static method ##################################*/


}
