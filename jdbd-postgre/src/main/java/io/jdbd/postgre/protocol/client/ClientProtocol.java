package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

public interface ClientProtocol {

    long getId();


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    Mono<ResultState> staticUpdate(Stmt stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     */
    Flux<ResultRow> staticQuery(Stmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     */
    Flux<ResultState> staticBatchUpdate(GroupStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     */
    MultiResult staticAsMulti(GroupStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     */
    Flux<Result> staticAsFlux(GroupStmt stmt);

    /*################################## blow for bindable single smt ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeUpdate()} method.
     * </p>
     */
    Mono<ResultState> bindableUpdate(BindableStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindableStatement#executeQuery()}</li>
     *     <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     */
    Flux<ResultRow> bindableQuery(BindableStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     */
    Flux<ResultState> bindableBatchUpdate(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsMulti()} method.
     * </p>
     */
    MultiResult bindableAsMulti(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsFlux()} method.
     * </p>
     */
    Flux<Result> bindableAsFlux(BatchBindStmt stmt);

    /*################################## blow for multi stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsMulti()} method.
     * </p>
     */
    MultiResult multiStmtAsMulti(MultiBindStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsFlux()} method.
     * </p>
     */
    Flux<Result> multiStmtAsFlux(MultiBindStmt stmt);


    /*################################## blow for sessioin ##################################*/


    Mono<Void> reset();


    Mono<Void> close();


}
