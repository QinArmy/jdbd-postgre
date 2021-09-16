package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgServerVersion;
import io.jdbd.postgre.Server;
import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.JdbdStmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

public abstract class ClientProtocolFactory {

    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }

    public static Mono<ClientProtocol> single(SessionAdjutant sessionAdjutant, int hostIndex) {
        return PgTaskExecutor.create(sessionAdjutant, hostIndex)// 1. create TCP connection.
                .map(executor -> new ConnectionManagerImpl(executor, hostIndex))
                .flatMap(ConnectionManagerImpl::connect) // 2. authentication and initializing
                .map(ClientProtocolImpl::create); // 3. create ClientProtocol instance.
    }


    private static final class ConnectionManagerImpl implements ConnectionManager {

        private static final Logger LOG = LoggerFactory.getLogger(ConnectionManagerImpl.class);

        final PgTaskExecutor executor;

        final int hostIndex;

        private ConnectionManagerImpl(PgTaskExecutor executor, int hostIndex) {
            this.executor = executor;
            this.hostIndex = hostIndex;
        }

        @Override
        public final Mono<Void> reConnect() {
            return Mono.empty();
        }

        @Override
        public final Mono<Void> reset(Map<String, String> initializedParamMap) {
            return Mono.empty();
        }

        @Override
        public final TaskAdjutant taskAdjutant() {
            return this.executor.taskAdjutant();
        }

        /*################################## blow private method ##################################*/

        private Mono<ConnectionWrapper> connect() {
            return authenticateAndInitializing()
                    .map(this::createWrapper);
        }

        private ConnectionWrapper createWrapper(final Map<String, String> initializedParamMap) {
            return new ConnectionWrapper(this, initializedParamMap);
        }

        private Mono<Map<String, String>> authenticateAndInitializing() {
            return PgConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(this.executor::handleAuthenticationSuccess)
                    .then(Mono.defer(this::initializing))
                    .flatMap(this::doOnInitializingSuccess);

        }

        private Mono<Map<String, String>> initializing() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties<PgKey> properties = adjutant.obtainHost().getProperties();
            final Server server = adjutant.server();
            final PgServerVersion serverVersion = server.serverVersion();

            final List<String> sqlGroup = new ArrayList<>(3);

            if (serverVersion.compareTo(PgServerVersion.V9_0) >= 0) {
                sqlGroup.add("SET extra_float_digits = 3");

                final String applicationName = properties.getProperty(PgKey.ApplicationName);
                if (PgStrings.hasText(applicationName)) {
                    sqlGroup.add(String.format("SET application_name = '%s'", applicationName));
                }
            }

            // 'SHOW ALL' must be last statement.
            sqlGroup.add("SHOW ALL");
            final int showResultIndex = sqlGroup.size() - 1;
            return Flux.from(SimpleQueryTask.batchAsFlux(JdbdStmts.group(sqlGroup), adjutant))
                    .switchIfEmpty(initializingFailure())
                    .filter(result -> result.getResultIndex() == showResultIndex)
                    .collectList()
                    .map(this::readInitializedParamResult);
        }


        private Mono<Map<String, String>> doOnInitializingSuccess(final Map<String, String> initializedParamMap) {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            if (LOG.isDebugEnabled()) {
                LOG.debug("database session initializing success,process id[{}].", adjutant.processId());
            }
            final String paramName = ServerParameter.lc_monetary.name();
            final Map<String, String> paramMap;
            paramMap = Collections.singletonMap(paramName, initializedParamMap.get(paramName));
            return this.executor.handleInitializingSuccess(paramMap)
                    .thenReturn(initializedParamMap);
        }


        /**
         * @param resultList result of  command 'SHOW ALL'
         * @return a unmodified map
         */
        private Map<String, String> readInitializedParamResult(final List<Result> resultList) {
            final Map<String, String> map = new HashMap<>((int) (resultList.size() / 0.75F));
            ResultRow row;
            for (Result result : resultList) {
                if (result instanceof ResultRow) {
                    row = (ResultRow) result;
                    map.put(row.get(0, String.class), row.get(1, String.class));
                }
            }
            if (map.isEmpty()) {
                throw new PgJdbdException("Session initializing failure,'SHOW ALL' execute failure.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Initialized server parameter count {} .", map.size());
            }
            return Collections.unmodifiableMap(map);
        }

        private static <T> Mono<T> initializingFailure() {
            return Mono.error(new PgJdbdException("Session initializing failure,SimpleQueryTask response error."));
        }


    }


}
