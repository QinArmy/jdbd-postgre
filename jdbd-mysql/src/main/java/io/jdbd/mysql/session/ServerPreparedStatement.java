package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.PrepareStmtTask;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.ReactorPreparedStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

public final class ServerPreparedStatement<S extends MySQLDatabaseSession> extends MySQLStatement<S>
        implements ReactorPreparedStatement {

    public static <S extends MySQLDatabaseSession> ServerPreparedStatement<S> create(S session, PrepareStmtTask task) {
        return new ServerPreparedStatement<>(session, task);
    }

    final S session;

    final PrepareStmtTask task;

    private ServerPreparedStatement(S session, PrepareStmtTask task) {
        super(session);
        this.session = session;
        this.task = task;
    }


    @Override
    public final void bind(int indexBasedZero, @Nullable Object nullable) {

    }

    @Override
    public void addBatch() {

    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public boolean supportLongData() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public final Flux<ResultStatus> executeBatch() {
        return null;
    }

    @Override
    public Mono<ResultStatus> executeUpdate() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStatus> statesConsumer) {
        return null;
    }

    @Override
    public ReactorMultiResult executeAsMulti() {
        return null;
    }


    @Override
    public Flux<SingleResult> executeAsFlux() {
        return null;
    }
}
