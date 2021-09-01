package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.stmt.BatchStmt;
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
    Flux<ResultStates> batchUpdate(BatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     */
    MultiResult batchAsMulti(BatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     */
    Flux<Result> batchAsFlux(BatchStmt stmt);

    Flux<Result> multiCommandAsFlux(StaticStmt stmt);

    /*################################## blow for bindable single smt ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     */
    Mono<ResultStates> bindableUpdate(BindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     */
    Flux<ResultRow> bindableQuery(BindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     */
    Flux<ResultStates> bindableBatchUpdate(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     */
    MultiResult bindableAsMulti(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     */
    Flux<Result> bindableAsFlux(BatchBindStmt stmt);

    /*################################## blow for multi stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     */
    MultiResult multiStmtAsMulti(MultiBindStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     */
    Flux<Result> multiStmtAsFlux(MultiBindStmt stmt);

    Mono<PreparedStatement> prepare(String sql, Function<PrepareStmtTask, PreparedStatement> function);


    /*################################## blow for session ##################################*/


    Mono<ClientProtocol> reset();


    Mono<Void> close();


}
