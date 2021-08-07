package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.Stmt;
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
    public final Mono<ResultState> update(Stmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultRow> query(Stmt stmt) {
        return SimpleQueryTask.query(stmt, this.adjutant);
    }

    @Override
    public final Mono<Void> reset() {
        return Mono.empty();
    }

    @Override
    public final Mono<Void> close() {
        return Mono.empty();
    }


}
