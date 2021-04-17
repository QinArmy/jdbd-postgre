package io.jdbd.mysql.protocol.client;

import io.jdbd.MultiResults;
import io.jdbd.PreparedStatement;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.stmt.BatchBindWrapper;
import io.jdbd.mysql.stmt.BindableWrapper;
import io.jdbd.vendor.conf.HostInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
public final class ClientCommandProtocolImpl implements ClientCommandProtocol {


    public static Mono<ClientCommandProtocol> create(HostInfo<PropertyKey> hostInfo
            , MySQLSessionAdjutant sessionAdjutant) {
//        return ClientConnectionProtocolImpl.create(hostInfo, sessionAdjutant)
//                .map(ClientCommandProtocolImpl::new);
        return Mono.empty();

    }

    private final MySQLTaskExecutor taskExecutor;

    private final SessionResetter sessionResetter;


    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl cp) {
        this.taskExecutor = cp.taskExecutor;
        this.sessionResetter = cp.sessionResetter;
    }


    /*################################## blow ClientCommandProtocol method ##################################*/

    @Override
    public long getId() {
        return this.taskExecutor.getAdjutant()
                .obtainHandshakeV10Packet()
                .getThreadId();
    }

    @Override
    public final Mono<ResultStates> update(String sql) {
        return ComQueryTask.update(sql, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Flux<ResultRow> query(String sql, Consumer<ResultStates> statesConsumer) {
        return ComQueryTask.query(sql, statesConsumer, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Flux<ResultStates> batchUpdate(List<String> sqlList) {
        return ComQueryTask.batchUpdate(sqlList, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Mono<ResultStates> bindableUpdate(BindableWrapper wrapper) {
        return ComQueryTask.bindableUpdate(wrapper, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Flux<ResultRow> bindableQuery(BindableWrapper wrapper) {
        return ComQueryTask.bindableQuery(wrapper, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Flux<ResultStates> bindableBatch(BatchBindWrapper wrapper) {
        return ComQueryTask.bindableBatch(wrapper, this.taskExecutor.getAdjutant());
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql) {
        return ComPreparedTask.prepare(sql, this.taskExecutor.getAdjutant());
    }

    @Override
    public final MultiResults multiStmt(List<String> commandList) {
        return ComQueryTask.multiStmt(commandList, this.taskExecutor.getAdjutant());
    }

    @Override
    public final MultiResults multiBindable(List<BindableWrapper> wrapperList) {
        return ComQueryTask.bindableMultiStmt(wrapperList, this.taskExecutor.getAdjutant());
    }

    @Override
    public Mono<Void> closeGracefully() {
        return QuitTask.quit(this.taskExecutor.getAdjutant());
    }

    @Override
    public Mono<Void> reset() {
        return this.sessionResetter.reset()
                .doOnSuccess(server -> MySQLTaskExecutor.resetTaskAdjutant(this.taskExecutor, server))
                .then()
                ;
    }

    /*################################## blow private method ##################################*/


}
