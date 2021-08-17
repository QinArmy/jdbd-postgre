package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.Server;
import io.jdbd.postgre.ServerVersion;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.JdbdStmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

public abstract class ClientProtocolFactory {

    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }

    public static Mono<ClientProtocol> single(SessionAdjutant sessionAdjutant, int hostIndex) {
        return PgTaskExecutor.create(sessionAdjutant, hostIndex)
                .map(executor -> new ConnectionManagerImpl(executor, sessionAdjutant, hostIndex))
                .flatMap(ConnectionManagerImpl::connect)
                .map(ClientProtocolImpl::create);
    }


    private static final class ConnectionManagerImpl implements ConnectionManager {

        private static final Logger LOG = LoggerFactory.getLogger(ConnectionManagerImpl.class);

        final PgTaskExecutor executor;

        final SessionAdjutant sessionAdjutant;

        final int hostIndex;

        private ConnectionManagerImpl(PgTaskExecutor executor, SessionAdjutant sessionAdjutant, int hostIndex) {
            this.executor = executor;
            this.sessionAdjutant = sessionAdjutant;
            this.hostIndex = hostIndex;
        }

        @Override
        public final Mono<Void> reConnect() {
            return Mono.empty();
        }

        @Override
        public final Mono<Server> reset() {
            return Mono.empty();
        }

        @Override
        public final TaskAdjutant taskAdjutant() {
            return this.executor.taskAdjutant();
        }

        /*################################## blow private method ##################################*/

        private Mono<ConnectionManagerImpl> connect() {
            return authenticateAndInitializing()
                    .thenReturn(this);
        }

        private Mono<Void> authenticateAndInitializing() {
            return PgConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(result -> PgTaskExecutor.handleAuthenticationSuccess(this.executor, result))
                    .then(Mono.defer(this::initializing))
                    .doOnSuccess(this::printInitializingSuccessLog);

        }

        private Mono<Void> initializing() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties<PgKey> properties = adjutant.obtainHost().getProperties();
            final Server server = adjutant.server();
            final ServerVersion serverVersion = server.serverVersion();

            final List<String> sqlGroup = new ArrayList<>(2);

            if (serverVersion.compareTo(ServerVersion.V9_0) >= 0) {
                sqlGroup.add("SET extra_float_digits = 3");

                final String applicationName = properties.getProperty(PgKey.ApplicationName);
                if (PgStrings.hasText(applicationName)) {
                    sqlGroup.add(String.format("SET application_name = '%s'", applicationName));
                }
            }


            final Mono<Void> resultMono;
            if (sqlGroup.isEmpty()) {
                resultMono = Mono.empty();
            } else {
                resultMono = SimpleQueryTask.batchUpdate(JdbdStmts.group(sqlGroup), adjutant)
                        .switchIfEmpty(Mono.defer(this::publishUpdateFailure))
                        .count()
                        .flatMap(count -> {
                            final Mono<Void> mono;
                            if (count == sqlGroup.size()) {
                                mono = Mono.empty();
                            } else {
                                mono = publishUpdateFailure();
                            }
                            return mono;
                        });
            }
            return resultMono;
        }

        private <T> Mono<T> publishUpdateFailure() {
            return Mono.error(new PgJdbdException("Session initializing failure,SimpleQueryTask response error."));
        }

        private void printInitializingSuccessLog(Void v) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("database session initializing success,process id[{}]."
                        , this.executor.taskAdjutant().processId());
            }
        }


    }


}
