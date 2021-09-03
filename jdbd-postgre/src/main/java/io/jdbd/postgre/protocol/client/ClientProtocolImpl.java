package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.stmt.BatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

final class ClientProtocolImpl implements ClientProtocol {

    static ClientProtocolImpl create(ConnectionManager connManager) {
        return new ClientProtocolImpl(connManager);
    }

    private final ConnectionManager connManager;

    final TaskAdjutant adjutant;


    private ClientProtocolImpl(ConnectionManager connManager) {
        this.connManager = connManager;
        this.adjutant = this.connManager.taskAdjutant();
    }

    @Override
    public final long getId() {
        return this.adjutant.processId();
    }

    @Override
    public final Mono<ResultStates> update(StaticStmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultRow> query(StaticStmt stmt) {
        return SimpleQueryTask.query(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultStates> batchUpdate(BatchStmt stmt) {
        return SimpleQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult batchAsMulti(BatchStmt stmt) {
        return SimpleQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> batchAsFlux(BatchStmt stmt) {
        return SimpleQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> multiCommandAsFlux(StaticStmt stmt) {
        return SimpleQueryTask.multiCommandAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Mono<ResultStates> bindUpdate(BindStmt stmt) {
        return SimpleQueryTask.bindableUpdate(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultRow> bindQuery(BindStmt stmt) {
        return SimpleQueryTask.bindableQuery(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultStates> bindBatch(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult bindBatchAsMulti(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableAsMulti(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> bindBatchAsFlux(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultStates> multiStmtBatch(MultiBindStmt stmt) {
        return SimpleQueryTask.multiStmtBatch(stmt, this.adjutant);
    }

    @Override
    public final MultiResult multiStmtAsMulti(MultiBindStmt stmt) {
        return SimpleQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> multiStmtAsFlux(MultiBindStmt stmt) {
        return SimpleQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql, Function<PrepareStmtTask, PreparedStatement> function) {
        return ExtendedQueryTask.prepare(sql, function, this.adjutant);
    }

    @Override
    public Mono<ClientProtocol> ping(final int timeSeconds) {
        //TODO FIX me
        return Mono.just(this);
    }

    @Override
    public final Mono<ClientProtocol> reset() {
        //TODO FIX me
        return Mono.just(this);
    }

    @Override
    public final Mono<Void> close() {
        return TerminateTask.terminate(this.adjutant);
    }


}
