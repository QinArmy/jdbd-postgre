package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    public final Mono<ResultState> update(StaticStmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultRow> query(StaticStmt stmt) {
        return SimpleQueryTask.query(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultState> batchUpdate(GroupStmt stmt) {
        return SimpleQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult batchAsMulti(GroupStmt stmt) {
        return SimpleQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> batchAsFlux(GroupStmt stmt) {
        return SimpleQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> multiCommandAsFlux(StaticStmt stmt) {
        return SimpleQueryTask.multiCommandAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Mono<ResultState> bindableUpdate(BindableStmt stmt) {
        return SimpleQueryTask.bindableUpdate(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultRow> bindableQuery(BindableStmt stmt) {
        return SimpleQueryTask.bindableQuery(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultState> bindableBatchUpdate(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult bindableAsMulti(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableAsMulti(stmt, this.adjutant);
    }

    @Override
    public final Flux<Result> bindableAsFlux(BatchBindStmt stmt) {
        return SimpleQueryTask.bindableAsFlux(stmt, this.adjutant);
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
    public final Mono<ClientProtocol> reset() {
        //TODO FIX me
        return Mono.just(this);
    }

    @Override
    public final Mono<Void> close() {
        return TerminateTask.terminate(this.adjutant);
    }


}
