package io.jdbd.postgre.protocol.client;

import io.jdbd.ServerVersion;
import io.jdbd.postgre.stmt.*;
import io.jdbd.postgre.util.PgBinds;
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
        final Mono<ResultStates> mono;
        if (PgBinds.hasPublisher(stmt.getBindGroup())) {
            mono = ExtendedQueryTask.update(stmt, this.adjutant);
        } else {
            mono = SimpleQueryTask.bindableUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public final Flux<ResultRow> bindQuery(final BindStmt stmt) {
        final Flux<ResultRow> flux;
        if (stmt.getFetchSize() > 0 || PgBinds.hasPublisher(stmt.getBindGroup())) {
            flux = ExtendedQueryTask.query(stmt, this.adjutant);
        } else {
            flux = SimpleQueryTask.bindableQuery(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public final Flux<ResultStates> bindBatch(final BindBatchStmt stmt) {
        final Flux<ResultStates> flux;
        if (PgBinds.hasPublisher(stmt)) {
            flux = ExtendedQueryTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = SimpleQueryTask.bindableBatchUpdate(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public final MultiResult bindBatchAsMulti(final BindBatchStmt stmt) {
        final MultiResult result;
        if ((stmt.getGroupList().size() == 1 && stmt.getFetchSize() > 0) || PgBinds.hasPublisher(stmt)) {
            result = ExtendedQueryTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = SimpleQueryTask.bindableAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public final OrderedFlux bindBatchAsFlux(BindBatchStmt stmt) {
        final OrderedFlux flux;
        if ((stmt.getGroupList().size() == 1 && stmt.getFetchSize() > 0) || PgBinds.hasPublisher(stmt)) {
            flux = ExtendedQueryTask.batchAsFlux(stmt, this.adjutant);
        } else {
            flux = SimpleQueryTask.bindableAsFlux(stmt, this.adjutant);
        }
        return flux;
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
