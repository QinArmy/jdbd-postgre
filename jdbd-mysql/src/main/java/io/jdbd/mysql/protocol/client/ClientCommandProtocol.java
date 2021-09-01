package io.jdbd.mysql.protocol.client;


import io.jdbd.DatabaseSession;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;


/**
 * <p>
 * This interface is underlying api of below interfaces:
 *     <ul>
 *         <li>{@link io.jdbd.TxDatabaseSession}</li>
 *         <li>{@link io.jdbd.xa.XaDatabaseSession}</li>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 */
public interface ClientCommandProtocol extends ClientProtocol {

    long getId();

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     *
     * @see ComQueryTask#update(StaticStmt, TaskAdjutant)
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
     *
     * @see ComQueryTask#query(StaticStmt, TaskAdjutant)
     */
    Flux<ResultRow> query(StaticStmt stmt);


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, TaskAdjutant)
     */
    Flux<ResultStates> batchUpdate(List<StaticStmt> stmtList);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     *
     * @see ComQueryTask#asMulti(List, TaskAdjutant)
     */
    ReactorMultiResult executeAsMulti(List<StaticStmt> stmtList);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     */
    Flux<SingleResult> executeAsFlux(List<StaticStmt> stmtList);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComQueryTask#bindableUpdate(BindableStmt, TaskAdjutant)
     */
    Mono<ResultStates> bindableUpdate(BindableStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ComQueryTask#bindableQuery(BindableStmt, TaskAdjutant)
     */
    Flux<ResultRow> bindableQuery(BindableStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, TaskAdjutant)
     */
    Flux<ResultStates> bindableBatch(BatchBindStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#bindableAsMulti(BatchBindStmt, TaskAdjutant)
     */
    ReactorMultiResult bindableAsMulti(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ComQueryTask#bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    Flux<SingleResult> bindableAsFlux(BatchBindStmt stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link DatabaseSession#prepare(String)}</li>
     *     <li>{@link DatabaseSession#prepare(String, int)}</li>
     * </ul>
     * </p>
     *
     * @see ComPreparedTask#prepare(MySQLDatabaseSession, StaticStmt, TaskAdjutant)
     */
    Mono<PreparedStatement> prepare(MySQLDatabaseSession session, StaticStmt stmt);


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#multiStmtAsMulti(List, TaskAdjutant)
     */
    MultiResult multiStmtAsMulti(List<BindableStmt> wrapperList);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ComQueryTask#multiStmtAsFlux(List, TaskAdjutant)
     */
    Flux<SingleResult> multiStmtAsFlux(List<BindableStmt> wrapperList);

    Mono<Void> reset();

}
