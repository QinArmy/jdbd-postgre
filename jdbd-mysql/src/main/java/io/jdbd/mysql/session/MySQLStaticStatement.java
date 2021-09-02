package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.StaticStatement;
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
        implements StaticStatement {


    static <S extends MySQLDatabaseSession> MySQLStaticStatement<S> create(S session) {
        return new MySQLStaticStatement<>(session);
    }

    private int timeout = 0;

    private MySQLStaticStatement(S session) {
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
    public final Flux<Result> executeAsFlux(List<String> sqlGroup) {
        return null;
    }


    @Override
    public final Flux<Result> executeAsFlux(String multiStmt) {
        throw new UnsupportedOperationException();
    }


    /*################################## blow private static method ##################################*/


}
