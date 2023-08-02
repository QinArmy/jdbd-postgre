package io.jdbd.postgre.protocol.client;

import io.jdbd.result.*;
import io.jdbd.session.HandleMode;
import io.jdbd.session.ServerVersion;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TransactionStatus;
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

    private final SessionManager connManager;

    final TaskAdjutant adjutant;

    private final Map<String, String> initializedParamMap;

    private ClientProtocol(final ConnectionWrapper wrapper) {
        this.connManager = wrapper.sessionManager;
        this.adjutant = this.connManager.taskAdjutant();
        this.initializedParamMap = wrapper.initializedParamMap;
    }

    @Override
    public long getId() {
        return this.adjutant.processId();
    }


    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return null;
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function) {
        return null;
    }

    @Override
    public BatchQuery batchQuery(StaticBatchStmt stmt) {
        return null;
    }

    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return null;
    }

    @Override
    public MultiResult batchAsMulti(StaticBatchStmt stmt) {
        return null;
    }

    @Override
    public OrderedFlux batchAsFlux(StaticBatchStmt stmt) {
        return null;
    }

    @Override
    public OrderedFlux executeAsFlux(StaticMultiStmt stmt) {
        return null;
    }

    @Override
    public Mono<ResultStates> bindUpdate(ParamStmt stmt, boolean usePrepare) {
        return null;
    }

    @Override
    public <R> Flux<R> bindQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function) {
        return null;
    }

    @Override
    public Flux<ResultStates> bindBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        return null;
    }

    @Override
    public BatchQuery bindBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        return null;
    }

    @Override
    public MultiResult bindBatchAsMulti(ParamBatchStmt stmt, boolean usePrepare) {
        return null;
    }

    @Override
    public OrderedFlux bindBatchAsFlux(ParamBatchStmt stmt, boolean usePrepare) {
        return null;
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return null;
    }

    @Override
    public BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt) {
        return null;
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return null;
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return null;
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
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
    public boolean supportSavePoints() {
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
    public Mono<Void> setTransactionOption(TransactionOption option, HandleMode mode) {
        return null;
    }

    @Override
    public boolean inTransaction() {
        return false;
    }

    @Override
    public Mono<ResultStates> commit() {
        return null;
    }

    @Override
    public Mono<ResultStates> rollback() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public Mono<Void> close() {
        return null;
    }


}
