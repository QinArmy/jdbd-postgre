package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindMultiStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SafePublisher;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ClientProtocol {

    long getId();


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    Mono<ResultStates> update(StaticStmt stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     */
    Flux<ResultRow> query(StaticStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     */
    Flux<ResultStates> batchUpdate(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     */
    MultiResult batchAsMulti(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     */
    SafePublisher batchAsFlux(StaticBatchStmt stmt);

    SafePublisher multiCommandAsFlux(StaticStmt stmt);

    /*################################## blow for binda single smt ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     */
    Mono<ResultStates> bindUpdate(BindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     */
    Flux<ResultRow> bindQuery(BindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     */
    Flux<ResultStates> bindBatch(BindBatchStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     */
    MultiResult bindBatchAsMulti(BindBatchStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     */
    SafePublisher bindBatchAsFlux(BindBatchStmt stmt);

    /*################################## blow for multi stmt ##################################*/

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
    SafePublisher multiStmtAsFlux(BindMultiStmt stmt);

    Mono<PreparedStatement> prepare(String sql, Function<PrepareStmtTask, PreparedStatement> function);


    /*################################## blow for session ##################################*/

    Mono<ClientProtocol> ping(int timeSeconds);


    Mono<ClientProtocol> reset();


    Mono<Void> close();


}
