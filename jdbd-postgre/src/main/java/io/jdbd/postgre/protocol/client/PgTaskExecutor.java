package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.util.List;

final class PgTaskExecutor extends CommunicationTaskExecutor<TaskAdjutant> {

    static Mono<PgTaskExecutor> create(final SessionAdjutant sessionAdjutant, final int hostIndex) {
        final List<PostgreHost> hostList = sessionAdjutant.obtainUrl().getHostList();

        final Mono<PgTaskExecutor> mono;
        if (hostIndex > -1 && hostIndex < hostList.size()) {
            final PostgreHost host = hostList.get(hostIndex);
            mono = TcpClient.create()
                    .runOn(sessionAdjutant.obtainEventLoopGroup())
                    .host(host.getHost())
                    .port(host.getPort())
                    .connect()
                    .map(connection -> new PgTaskExecutor(connection, host, sessionAdjutant));
        } else {
            IllegalArgumentException e = new IllegalArgumentException(
                    String.format("hostIndex[%s] not in [0,%s)", hostIndex, hostList.size()));
            mono = Mono.error(new PgJdbdException("Not found HostInfo in url.", e));
        }
        return mono;
    }

    static void handleAuthenticationSuccess(PgTaskExecutor executor, AuthResult result) {
        LOG.debug("Authentication success.");
    }


    private static final Logger LOG = LoggerFactory.getLogger(PgTaskExecutor.class);


    private final PostgreHost host;

    private final SessionAdjutant sessionAdjutant;

    private PgTaskExecutor(Connection connection, PostgreHost host, SessionAdjutant sessionAdjutant) {
        super(connection);
        this.host = host;
        this.sessionAdjutant = sessionAdjutant;
    }

    @Override
    protected final Logger obtainLogger() {
        return LOG;
    }

    @Override
    protected final void updateServerStatus(Object serverStatus) {

    }

    @Override
    protected final TaskAdjutant createTaskAdjutant() {
        return new TaskAdjutantWrapper(this);
    }

    @Override
    protected final HostInfo<?> obtainHostInfo() {
        return this.host;
    }

    @Override
    protected final boolean clearChannel(ByteBuf cumulateBuffer, Class<? extends CommunicationTask> taskClass) {
        return false;
    }


    /*################################## blow private static class ##################################*/


    private static final class TaskAdjutantWrapper extends AbstractTaskAdjutant implements TaskAdjutant {

        private final PgTaskExecutor taskExecutor;

        private TaskAdjutantWrapper(PgTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
        }

        @Override
        public final PostgreHost obtainHost() {
            return this.taskExecutor.host;
        }


    }


}
