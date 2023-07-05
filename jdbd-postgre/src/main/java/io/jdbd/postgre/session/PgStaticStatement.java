package io.jdbd.postgre.session;

import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.StaticStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

final class PgStaticStatement extends PgStatement implements StaticStatement {

    static PgStaticStatement create(PgDatabaseSession session) {
        return new PgStaticStatement(session);
    }

    private PgStaticStatement(PgDatabaseSession session) {
        super(session);
    }

    @Override
    public final Mono<ResultStates> executeUpdate(String sql) {
        return this.session.protocol.update(PgStmts.stmt(sql, this));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql) {
        return this.session.protocol.query(PgStmts.stmt(sql, this));
    }

    @Override
    public final Flux<ResultRow> executeQuery(String sql, Consumer<ResultStates> statesConsumer) {
        return this.session.protocol.query(PgStmts.stmt(sql, statesConsumer, this));
    }

    @Override
    public final Flux<ResultStates> executeBatchUpdate(List<String> sqlGroup) {
        return this.session.protocol.batchUpdate(PgStmts.batch(sqlGroup, this));
    }

    @Override
    public final MultiResult executeBatchAsMulti(List<String> sqlGroup) {
        return this.session.protocol.batchAsMulti(PgStmts.batch(sqlGroup, this));
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(List<String> sqlGroup) {
        return this.session.protocol.batchAsFlux(PgStmts.batch(sqlGroup, this));
    }

    @Override
    public final OrderedFlux executeAsFlux(String multiStmt) {
        return this.session.protocol.multiCommandAsFlux(PgStmts.stmt(multiStmt, this));
    }

    /*################################## blow Statement method ##################################*/

    @Override
    public final boolean supportPublisher() {
        return false;
    }

    /*################################## blow StatementOption method ##################################*/


    @Override
    public final boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public final int getFetchSize() {
        return 0;
    }


}
