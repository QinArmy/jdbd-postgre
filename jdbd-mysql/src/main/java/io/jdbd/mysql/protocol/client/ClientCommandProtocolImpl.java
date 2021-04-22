package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.Stmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientCommandProtocolImpl implements ClientCommandProtocol {


    public static Mono<ClientCommandProtocol> create(HostInfo<PropertyKey> hostInfo
            , MySQLSessionAdjutant sessionAdjutant) {
//        return ClientConnectionProtocolImpl.create(hostInfo, sessionAdjutant)
//                .map(ClientCommandProtocolImpl::new);
        return Mono.empty();

    }

    private final MySQLTaskExecutor executor;

    private final MySQLTaskAdjutant adjutant;

    private final SessionResetter sessionResetter;


    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl cp) {
        this.executor = cp.taskExecutor;
        this.adjutant = this.executor.getAdjutant();
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
    public final Mono<ResultStatus> update(Stmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultRow> query(Stmt stmt) {
        return ComQueryTask.query(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultStatus> batchUpdate(List<Stmt> stmtList) {
        return ComQueryTask.batchUpdate(stmtList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReactorMultiResult executeAsMulti(List<Stmt> stmtList) {
        return ComQueryTask.asMulti(stmtList, this.adjutant);
    }

    @Override
    public Flux<SingleResult> executeAsFlux(List<Stmt> stmtList) {
        return ComQueryTask.asFlux(stmtList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<ResultStatus> bindableUpdate(BindableStmt wrapper) {
        return ComQueryTask.bindableUpdate(wrapper, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultRow> bindableQuery(BindableStmt wrapper) {
        return ComQueryTask.bindableQuery(wrapper, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<ResultStatus> bindableBatch(BatchBindStmt stmt) {
        return ComQueryTask.bindableBatch(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReactorMultiResult bindableAsMulti(BatchBindStmt stmt) {
        return ComQueryTask.bindableAsMulti(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<SingleResult> bindableAsFlux(BatchBindStmt stmt) {
        return ComQueryTask.bindableAsFlux(stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<PreparedStatement> prepare(MySQLDatabaseSession session, Stmt stmt) {
        return ComPreparedTask.prepare(session, stmt, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final MultiResult multiStmtAsMulti(List<BindableStmt> wrapperList) {
        return ComQueryTask.multiStmtAsMulti(wrapperList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Flux<SingleResult> multiStmtAsFlux(List<BindableStmt> wrapperList) {
        return ComQueryTask.multiStmtAsFlux(wrapperList, this.adjutant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Mono<Void> closeGracefully() {
        return QuitTask.quit(this.executor.getAdjutant());
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
