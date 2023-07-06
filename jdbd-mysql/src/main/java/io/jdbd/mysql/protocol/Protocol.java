package io.jdbd.mysql.protocol;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.ServerVersion;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TransactionStatus;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticMultiStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Protocol {


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    Mono<ResultStates> update(StaticStmt stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * </p>
     */
    <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function, Consumer<ResultStates> consumer);


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatchUpdate(List)} method.
     * </p>
     */
    Flux<ResultStates> batchUpdate(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatchAsMulti(List)} method.
     * </p>
     */
    MultiResult batchAsMulti(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatchAsFlux(List)} method.
     * </p>
     */
    OrderedFlux batchAsFlux(StaticBatchStmt stmt);

    OrderedFlux executeAsFlux(StaticMultiStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     */
    Mono<ResultStates> bindUpdate(BindStmt stmt, boolean forcePrepare);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * </p>
     */
    <R> Flux<R> bindQuery(BindStmt stmt, boolean forcePrepare, Function<CurrentRow, R> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchUpdate()} method.
     * </p>
     */
    Flux<ResultStates> bindBatch(BindBatchStmt stmt, boolean forcePrepare);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     */
    MultiResult bindBatchAsMulti(BindBatchStmt stmt, boolean forcePrepare);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     */
    OrderedFlux bindBatchAsFlux(BindBatchStmt stmt, boolean forcePrepare);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchUpdate()} method.
     * </p>
     */
    Flux<ResultStates> multiStmtBatch(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     */
    MultiResult multiStmtAsMulti(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     */
    OrderedFlux multiStmtAsFlux(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link DatabaseSession#prepare(String)} methods:
     * </p>
     */
    Mono<PreparedStatement> prepare(String sql, Function<PrepareTask<MySQLType>, PreparedStatement> function);

    Mono<TransactionStatus> getTransactionOption();

    Mono<Void> startTransaction(TransactionOption option);

    Mono<Void> setTransactionOption(TransactionOption option);

    Mono<Void> commit();

    Mono<Void> rollback();


    Mono<Void> reset();

    Mono<Void> ping(int timeSeconds);

    boolean startedTransaction(ResultStates states);

    boolean isReadOnlyTransaction(ResultStates states);

    boolean supportMultiStmt();

    ServerVersion getServerVersion();

    boolean isClosed();

    Mono<Void> close();

}
