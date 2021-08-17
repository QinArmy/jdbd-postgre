package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.*;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.nio.charset.Charset;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

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
        ((TaskAdjutantWrapper) executor.taskAdjutant).authenticationSuccess(result);
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
    protected final void updateServerStatus(final Object serverStatus) {
        final TaskAdjutantWrapper adjutant = (TaskAdjutantWrapper) this.taskAdjutant;
        if (serverStatus instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, String> statusMap = (Map<String, String>) serverStatus;
            adjutant.updateServerParameterStatus(statusMap);
        } else if (serverStatus instanceof TxStatus) {
            adjutant.updateTxStatus((TxStatus) serverStatus);
        }

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

        private ServerImpl server;

        private TxStatus txStatus;

        private PgParser parser;

        private BackendKeyData backendKeyData;

        private TaskAdjutantWrapper(PgTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
        }

        @Override
        public final PostgreHost obtainHost() {
            return this.taskExecutor.host;
        }

        @Override
        public final long processId() {
            final BackendKeyData keyData = this.backendKeyData;
            if (keyData == null) {
                throw new IllegalStateException("this.backendKeyData is null.");
            }
            return keyData.processId;
        }

        @Override
        public final int serverSecretKey() {
            final BackendKeyData keyData = this.backendKeyData;
            if (keyData == null) {
                throw new IllegalStateException("this.backendKeyData is null.");
            }
            return keyData.secretKey;
        }

        @Override
        public final Charset clientCharset() {
            return Encoding.CLIENT_CHARSET;
        }

        @Override
        public final ZoneOffset clientOffset() {
            return PgTimes.systemZoneOffset();
        }

        @Override
        public final PgParser sqlParser() {
            final PgParser parser = this.parser;
            if (parser == null) {
                throw new IllegalStateException("this.parser is null.");
            }
            return parser;
        }

        @Override
        public final TxStatus txStatus() {
            final TxStatus txStatus = this.txStatus;
            if (txStatus == null) {
                throw new IllegalStateException("this.txStatus is null");
            }
            return txStatus;
        }

        @Override
        public final Server server() {
            final Server server = this.server;
            if (server == null) {
                throw new IllegalStateException("this.server is null");
            }
            return server;
        }

        private void authenticationSuccess(AuthResult result) {
            synchronized (this) {
                this.server = new ServerImpl(result.serverStatusMap);
                this.backendKeyData = Objects.requireNonNull(result.backendKeyData, "result.backendKeyData");
                this.txStatus = Objects.requireNonNull(result.txStatus, "txStatus");
                this.parser = PgParser.create(this.server::parameter);
            }
        }

        private void updateServerParameterStatus(Map<String, String> paramStatusMap) {
            ServerImpl oldServer = Objects.requireNonNull(this.server, "this.server");
            this.server = new ServerImpl(oldServer, paramStatusMap);
        }

        private void updateTxStatus(TxStatus txStatus) {
            this.txStatus = txStatus;
        }


    }

    private static final class ServerImpl implements Server {

        private final ServerVersion serverVersion;

        private final Map<String, String> paramStatusMap;

        private final ZoneOffset zoneOffset;

        private ServerImpl(final Map<String, String> paramStatusMap) {
            this.paramStatusMap = Collections.unmodifiableMap(paramStatusMap);
            this.serverVersion = ServerVersion.from(paramStatusMap.get(ServerParameter.server_version.name()));
            final ZoneId zoneId = ZoneId.of(paramStatusMap.get(ServerParameter.TimeZone.name()), ZoneId.SHORT_IDS);
            this.zoneOffset = PgTimes.toZoneOffset(zoneId);
        }

        private ServerImpl(ServerImpl server, final Map<String, String> newStatusMap) {
            final Map<String, String> paramMap = new HashMap<>(server.paramStatusMap);
            paramMap.putAll(newStatusMap);

            this.paramStatusMap = Collections.unmodifiableMap(paramMap);
            this.serverVersion = server.serverVersion;

            final ZoneId zoneId = ZoneId.of(paramStatusMap.get(ServerParameter.TimeZone.name()), ZoneId.SHORT_IDS);
            this.zoneOffset = PgTimes.toZoneOffset(zoneId);
        }


        @Override
        public final ServerVersion serverVersion() {
            return this.serverVersion;
        }

        @Override
        public final String parameter(ServerParameter parameter) {
            return this.paramStatusMap.get(parameter.name());
        }

        @Override
        public final ZoneOffset zoneOffset() {
            return this.zoneOffset;
        }


    }


}
