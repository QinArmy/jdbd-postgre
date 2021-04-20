package io.jdbd.mysql.protocol.client;


import io.jdbd.DatabaseSession;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.StmtWrapper;
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
 *         <li>{@link BindableStatement}</li>
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
     * @see ComQueryTask#update(StmtWrapper, MySQLTaskAdjutant)
     */
    Mono<ResultStatus> update(StmtWrapper stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ComQueryTask#query(StmtWrapper, MySQLTaskAdjutant)
     */
    Flux<ResultRow> query(StmtWrapper stmt);


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    Flux<ResultStatus> batchUpdate(List<StmtWrapper> stmtList);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     *
     * @see ComQueryTask#asMulti(List, MySQLTaskAdjutant)
     */
    ReactorMultiResult executeAsMulti(List<StmtWrapper> stmtList);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     */
    Flux<SingleResult> executeAsFlux(List<StmtWrapper> stmtList);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComQueryTask#bindableUpdate(BindableStmt, MySQLTaskAdjutant)
     */
    Mono<ResultStatus> bindableUpdate(BindableStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindableStatement#executeQuery()}</li>
     *     <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ComQueryTask#bindableQuery(BindableStmt, MySQLTaskAdjutant)
     */
    Flux<ResultRow> bindableQuery(BindableStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    Flux<ResultStatus> bindableBatch(BatchBindStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#bindableAsMulti(BatchBindStmt, MySQLTaskAdjutant)
     */
    ReactorMultiResult bindableAsMulti(BatchBindStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsFlux()} method.
     * </p>
     *
     * @see ComQueryTask#bindableAsFlux(BatchBindStmt, MySQLTaskAdjutant)
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
     * @see ComPreparedTask#prepare(MySQLDatabaseSession, StmtWrapper, MySQLTaskAdjutant)
     */
    Mono<PreparedStatement> prepare(MySQLDatabaseSession session, StmtWrapper stmt);


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#bindableMultiStmt(List, MySQLTaskAdjutant)
     */
    MultiResult bindableMultiStmt(List<BindableStmt> wrapperList);

    Mono<Void> reset();

}
