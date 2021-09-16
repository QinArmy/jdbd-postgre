package io.jdbd.postgre.protocol.client;

import io.jdbd.ServerVersion;
import io.jdbd.postgre.stmt.*;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

final class ClientProtocolImpl implements ClientProtocol {

    static ClientProtocolImpl create(final ConnectionWrapper wrapper) {
        validateParamMap(wrapper.initializedParamMap);
        return new ClientProtocolImpl(wrapper);
    }

    private static void validateParamMap(Map<String, String> paramMap) {
        if (paramMap.isEmpty()) {
            throw new IllegalArgumentException("Initialized map is empty");
        }
        try {
            paramMap.put("This is a no-exists key,only test.", "");
            throw new IllegalArgumentException("Initialized map isn't unmodified map.");
        } catch (UnsupportedOperationException e) {
            // ok
        }
    }

    private final ConnectionManager connManager;

    final TaskAdjutant adjutant;

    private final Map<String, String> initializedParamMap;

    private ClientProtocolImpl(final ConnectionWrapper wrapper) {
        this.connManager = wrapper.connectionManager;
        this.adjutant = this.connManager.taskAdjutant();
        this.initializedParamMap = wrapper.initializedParamMap;
    }

    @Override
    public final long getId() {
        return this.adjutant.processId();
    }

    @Override
    public final ServerVersion getServerVersion() {
        return this.adjutant.server().serverVersion();
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
    public final Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult batchAsMulti(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public final OrderedFlux batchAsFlux(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public final OrderedFlux multiCommandAsFlux(StaticStmt stmt) {
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
    public final Flux<ResultStates> bindBatch(BindBatchStmt stmt) {
        return SimpleQueryTask.bindableBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public final MultiResult bindBatchAsMulti(BindBatchStmt stmt) {
        return SimpleQueryTask.bindableAsMulti(stmt, this.adjutant);
    }

    @Override
    public final OrderedFlux bindBatchAsFlux(BindBatchStmt stmt) {
        return SimpleQueryTask.bindableAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Flux<ResultStates> multiStmtBatch(BindMultiStmt stmt) {
        return SimpleQueryTask.multiStmtBatch(stmt, this.adjutant);
    }

    @Override
    public final MultiResult multiStmtAsMulti(BindMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public final OrderedFlux multiStmtAsFlux(BindMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql, Function<PrepareStmtTask, PreparedStatement> function) {
        return ExtendedQueryTask.prepare(sql, function, this.adjutant);
    }

    @Override
    public Mono<ClientProtocol> ping(final int timeSeconds) {
        // postgre no ping message.
        return SimpleQueryTask.query(PgStmts.stmt("SELECT 1 AS result ", timeSeconds), this.adjutant)
                .then(Mono.just(this));
    }

    @Override
    public final Mono<ClientProtocol> reset() {
        return this.connManager.reset(this.initializedParamMap)
                .thenReturn(this);
    }

    @Override
    public final Mono<Void> close() {
        return TerminateTask.terminate(this.adjutant);
    }


}
