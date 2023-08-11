package io.jdbd.postgre.protocol.client;

import io.jdbd.result.*;
import io.jdbd.session.*;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

final class ClientProtocol implements PgProtocol {

    static ClientProtocol create(final ConnectionWrapper wrapper) {
        validateParamMap(wrapper.initializedParamMap);
        return new ClientProtocol(wrapper);
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

    private final ProtocolManager protocolManager;
    private final TaskAdjutant adjutant;

    private final Map<String, String> initializedParamMap;

    private ClientProtocol(ConnectionWrapper wrapper) {
        this.protocolManager = wrapper.sessionManager;
        this.adjutant = this.protocolManager.taskAdjutant();
        this.initializedParamMap = wrapper.initializedParamMap;
    }

    @Override
    public long identifier() {
        return this.adjutant.processId();
    }

    @Override
    public void bindIdentifier(StringBuilder builder, String identifier) {
        //TODO
    }

    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function) {
        return SimpleQueryTask.query(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery batchQuery(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult batchAsMulti(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux executeAsFlux(StaticMultiStmt stmt) {
        return SimpleQueryTask.executeAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> paramUpdate(ParamStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.update(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramUpdate(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> paramQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function) {
        if (usePrepare) {
            return ExtendedQueryTask.query(stmt, function, this.adjutant);
        }
        return SimpleQueryTask.paramQuery(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> paramBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchUpdate(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery paramBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchQuery(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult paramBatchAsMulti(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchAsMulti(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux paramBatchAsFlux(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchAsFlux(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchAsFlux(stmt, this.adjutant);
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
        return ExtendedQueryTask.prepare(sql, this.adjutant);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
     */
    @Override
    public Mono<RefCursor> declareCursor(final StaticStmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant)
                .flatMap(states -> createCursor(states, stmt));
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
     */
    @Override
    public Mono<RefCursor> paramDeclareCursor(ParamStmt stmt, boolean useServerPrepare) {
        return null;
    }

    @Override
    public Mono<TransactionStatus> transactionStatus() {
        return null;
    }

    @Override
    public Mono<Void> ping(int timeSeconds) {
        return null;
    }

    @Override
    public Mono<Void> reset() {
        return null;
    }

    @Override
    public Mono<Void> reconnect() {
        return null;
    }

    @Override
    public boolean supportMultiStmt() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public boolean supportStmtVar() {
        return false;
    }

    @Override
    public ServerVersion serverVersion() {
        return null;
    }

    @Override
    public Mono<ResultStates> startTransaction(TransactionOption option, HandleMode mode) {
        return null;
    }

    @Override
    public boolean inTransaction() {
        return false;
    }

    @Override
    public Mono<ResultStates> commit(Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public Mono<ResultStates> rollback(Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> Mono<T> close() {
        if (!this.adjutant.isActive()) {
            return Mono.empty();
        }
        return TerminateTask.terminate(this.adjutant);
    }


    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }


    private Mono<RefCursor> createCursor(ResultStates states, SingleStmt stmt) {
        throw new UnsupportedOperationException();
    }


}
