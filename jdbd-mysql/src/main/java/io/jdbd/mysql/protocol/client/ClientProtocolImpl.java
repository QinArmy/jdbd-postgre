package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.StaticStmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientProtocolImpl implements ClientProtocol {


    public static Mono<ClientProtocol> create(HostInfo<MyKey> hostInfo
            , SessionAdjutant sessionAdjutant) {
//        return ClientConnectionProtocolImpl.create(hostInfo, sessionAdjutant)
//                .map(ClientCommandProtocolImpl::new);
        return Mono.empty();

    }

    private final MySQLTaskExecutor executor;

    private final TaskAdjutant adjutant;

    private final SessionResetter sessionResetter;


    private ClientProtocolImpl(ClientConnectionProtocolImpl cp) {
        this.executor = cp.taskExecutor;
        this.adjutant = this.executor.taskAdjutant();
        this.sessionResetter = cp.sessionResetter;
    }


    /*################################## blow ClientCommandProtocol method ##################################*/

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getId() {
        return this.adjutant
                .obtainHandshakeV10Packet()
                .getThreadId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<ResultStates> update(StaticStmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultRow> query(StaticStmt stmt) {
        return ComQueryTask.query(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultStates> batchUpdate(List<StaticStmt> stmtList) {
        return ComQueryTask.batchUpdate(stmtList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReactorMultiResult executeAsMulti(List<StaticStmt> stmtList) {
        return ComQueryTask.batchAsMulti(stmtList, this.adjutant);
    }

    @Override
    public Flux<SingleResult> executeAsFlux(List<StaticStmt> stmtList) {
        return ComQueryTask.batchAsFlux(stmtList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<ResultStates> bindableUpdate(BindStmt wrapper) {
        return ComQueryTask.bindableUpdate(wrapper, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultRow> bindableQuery(BindStmt wrapper) {
        return ComQueryTask.bindableQuery(wrapper, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultStates> bindableBatch(BindBatchStmt stmt) {
        return ComQueryTask.bindableBatch(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReactorMultiResult bindableAsMulti(BindBatchStmt stmt) {
        return ComQueryTask.bindableAsMulti(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<SingleResult> bindableAsFlux(BindBatchStmt stmt) {
        return ComQueryTask.bindableAsFlux(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<PreparedStatement> prepare(MySQLDatabaseSession session, StaticStmt stmt) {
        return ComPreparedStmtTask.prepare(session, stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final MultiResult multiStmtAsMulti(List<BindStmt> wrapperList) {
        return ComQueryTask.multiStmtAsMulti(wrapperList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<SingleResult> multiStmtAsFlux(List<BindStmt> wrapperList) {
        return ComQueryTask.multiStmtAsFlux(wrapperList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<Void> closeGracefully() {
        return QuitTask.quit(this.executor.taskAdjutant());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<Void> reset() {
        return this.sessionResetter.reset()
                .doOnSuccess(this::resetTaskAdjutant)
                .then();
    }

    /*################################## blow private method ##################################*/

    private void resetTaskAdjutant(Server server) {
        MySQLTaskExecutor.resetTaskAdjutant(this.executor, server);
    }


}
