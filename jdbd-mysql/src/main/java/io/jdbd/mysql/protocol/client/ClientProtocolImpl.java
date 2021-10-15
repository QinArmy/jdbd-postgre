package io.jdbd.mysql.protocol.client;

import io.jdbd.ServerVersion;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.SavePoint;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticMultiStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

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

    final TaskAdjutant adjutant;

    private final SessionResetter sessionResetter;


    private ClientProtocolImpl(ClientConnectionProtocolImpl cp) {
        this.executor = cp.taskExecutor;
        this.adjutant = this.executor.taskAdjutant();
        this.sessionResetter = cp.sessionResetter;
    }


    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public long getId() {
        return this.adjutant
                .handshake10()
                .getThreadId();
    }


    @Override
    public Mono<ResultStates> update(final StaticStmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public Flux<ResultRow> query(final StaticStmt stmt) {
        return ComQueryTask.query(stmt, this.adjutant);
    }

    @Override
    public Flux<ResultStates> batchUpdate(final StaticBatchStmt stmt) {
        return ComQueryTask.batchUpdate(stmt, this.adjutant);
    }


    @Override
    public MultiResult batchAsMulti(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux executeAsFlux(final StaticMultiStmt stmt) {
        return ComQueryTask.executeAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> bindableUpdate(final BindStmt stmt) {
        final Mono<ResultStates> mono;
        if (usePrepare(stmt.getBindGroup())) {
            mono = ComPreparedTask.update(stmt, this.adjutant);
        } else {
            mono = ComQueryTask.bindableUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public Flux<ResultRow> bindableQuery(final BindStmt stmt) {
        final Flux<ResultRow> flux;
        if (stmt.getFetchSize() > 0 || usePrepare(stmt.getBindGroup())) {
            flux = ComPreparedTask.query(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.bindableQuery(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> bindableBatch(final BindBatchStmt stmt) {
        final Flux<ResultStates> flux;
        if (usePrepare(stmt)) {
            flux = ComPreparedTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.bindableBatch(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public MultiResult bindableAsMulti(final BindBatchStmt stmt) {
        final MultiResult result;
        if (usePrepare(stmt)) {
            result = ComPreparedTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = ComQueryTask.bindableAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public OrderedFlux bindableAsFlux(final BindBatchStmt stmt) {
        final OrderedFlux flux;
        if ((stmt.getGroupList().size() == 1 && stmt.getFetchSize() > 0) || usePrepare(stmt)) {
            flux = ComPreparedTask.batchAsFlux(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.bindableAsFlux(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Mono<PreparedStatement> prepare(String sql, Function<PrepareTask<MySQLType>, PreparedStatement> function) {
        return ComPreparedTask.prepare(sql, this.adjutant, function);
    }

    @Override
    public MultiResult multiStmtAsMulti(final BindMultiStmt stmt) {
        return ComQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }


    @Override
    public OrderedFlux multiStmtAsFlux(final BindMultiStmt stmt) {
        return ComQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }


    @Override
    public Mono<Void> close() {
        return QuitTask.quit(this.executor.taskAdjutant());
    }


    @Override
    public Mono<Void> reset() {
        return this.sessionResetter.reset()
                .doOnSuccess(this::resetTaskAdjutant)
                .then();
    }


    @Override
    public boolean supportMultiStmt() {
        return Capabilities.supportMultiStatement(this.adjutant.negotiatedCapability());
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.adjutant.handshake10().getServerVersion();
    }

    @Override
    public boolean isClosed() {
        return !this.adjutant.isActive();
    }

    @Override
    public Mono<SavePoint> setSavepoint() {
        return null;
    }

    @Override
    public Mono<SavePoint> setSavepoint(String name) {
        return null;
    }

    @Override
    public Mono<Void> releaseSavePoint(SavePoint savepoint) {
        return null;
    }

    @Override
    public Mono<Void> rollbackToSavePoint(SavePoint savepoint) {
        return null;
    }

    /*################################## blow private method ##################################*/

    private void resetTaskAdjutant(Server server) {
        MySQLTaskExecutor.resetTaskAdjutant(this.executor, server);
    }

    private boolean usePrepare(final BindBatchStmt stmt) {
        boolean prepare = false;
        for (List<BindValue> group : stmt.getGroupList()) {
            if (usePrepare(group)) {
                prepare = true;
                break;
            }
        }
        return prepare;
    }

    private boolean usePrepare(final List<BindValue> group) {
        boolean prepare = false;
        Object value;
        outFor:
        for (BindValue bindValue : group) {
            value = bindValue.get();
            if (value == null) {
                continue;
            }
            if (value instanceof Publisher) {
                prepare = true;
                break;
            }
            switch (bindValue.getType()) {
                case LONGBLOB:
                case LONGTEXT: {
                    if (value instanceof Path) {
                        prepare = true;
                        break outFor;
                    }
                }
                break;
                default:
            }
        }
        return prepare;
    }


}
