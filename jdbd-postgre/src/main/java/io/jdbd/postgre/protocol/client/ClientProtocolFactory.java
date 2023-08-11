package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgServerVersion;
import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.env.PgHost;
import io.jdbd.postgre.protocol.PgProtocolFactory;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.env.Properties;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public final class ClientProtocolFactory extends FixedEnv implements PgProtocolFactory {


    public static ClientProtocolFactory create(PgHost host) {
        throw new UnsupportedOperationException();
    }


    final ConcurrentMap<String, PgStatement> stmtMap = PgCollections.concurrentHashMap();


    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }

    public static Mono<PgProtocol> single(final SessionAdjutant sessionAdjutant, final int hostIndex) {
        return PgTaskExecutor.create(sessionAdjutant, hostIndex)// 1. create TCP connection.
                .flatMap(executor -> connect(executor, hostIndex)) // 2. authentication and initializing
                .map(ClientProtocol::create); // 3. create ClientProtocol instance.
    }

    private static Mono<ConnectionWrapper> connect(PgTaskExecutor executor, int hostIndex) {
        final SessionManagerImpl connectionManager = new SessionManagerImpl(executor, hostIndex);
        return connectionManager.connect();
    }

    private static final class SessionManagerImpl implements ProtocolManager {


        private static final Logger LOG = LoggerFactory.getLogger(SessionManagerImpl.class);

        private static final Set<ServerParameter> INITIALIZED_PARAM_SET = Collections.unmodifiableSet(EnumSet.of(
                ServerParameter.lc_monetary,
                ServerParameter.transaction_isolation,
                ServerParameter.transaction_read_only,
                ServerParameter.transaction_deferrable,

                ServerParameter.statement_timeout
        ));

        private static final Set<ServerParameter> SENSITIVE_PARAM_SET = Collections.unmodifiableSet(EnumSet.of(
                ServerParameter.lc_monetary,
                ServerParameter.statement_timeout
        ));

        final PgTaskExecutor executor;

        final int hostIndex;

        private SessionManagerImpl(PgTaskExecutor executor, int hostIndex) {
            this.executor = executor;
            this.hostIndex = hostIndex;
        }

        @Override
        public Mono<Void> reConnect() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> reset(final Map<String, String> initializedParamMap) {
            final List<String> sqlGroup = new ArrayList<>(initializedParamMap.size());
            for (Map.Entry<String, String> e : initializedParamMap.entrySet()) {
                sqlGroup.add(String.format("SET %s = '%s'", e.getKey(), e.getValue()));
            }
            return SimpleQueryTask.batchUpdate(PgStmts.batch(sqlGroup), this.executor.taskAdjutant())
                    .then();
        }

        @Override
        public TaskAdjutant taskAdjutant() {
            return this.executor.taskAdjutant();
        }

        /*################################## blow private method ##################################*/

        /**
         * @see ClientProtocolFactory#single(SessionAdjutant, int)
         */
        private Mono<ConnectionWrapper> connect() {
            return PgConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(this.executor::handleAuthenticationSuccess)
                    .then(Mono.defer(this::initializing))
                    .flatMap(this::doOnInitializingSuccess)
                    .map(this::createWrapper);
        }

        /**
         * @see #connect()
         */
        private ConnectionWrapper createWrapper(final Map<String, String> initializedParamMap) {
            return new ConnectionWrapper(this, initializedParamMap);
        }

        /**
         * @see #connect()
         */
        private Mono<Map<String, String>> initializing() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.obtainHost().getProperties();
            final ServerEnv server = adjutant.server();
            final PgServerVersion serverVersion = server.serverVersion();

            final List<String> sqlGroup = new ArrayList<>(3);

            if (serverVersion.compareTo(PgServerVersion.V9_0) >= 0) {
                sqlGroup.add("SET extra_float_digits = 3");

                final String applicationName = properties.get(PgKey0.ApplicationName);
                if (PgStrings.hasText(applicationName)) {
                    sqlGroup.add(String.format("SET application_name = '%s'", applicationName));
                }
            }

            final String lcMonetary = properties.get(PgKey0.lc_monetary);
            if (PgStrings.hasText(lcMonetary)) {
                if (!PgStrings.isSafeParameterValue(lcMonetary)) {
                    return Mono.error(new JdbdException(String.format("lc_monetary[%s] error.", lcMonetary)));
                }
                sqlGroup.add(String.format("SET lc_monetary = '%s'", lcMonetary));
            }
            // 'SHOW xxx' must be last statement.
            final int showResultIndex = sqlGroup.size();

            for (ServerParameter parameter : INITIALIZED_PARAM_SET) {
                sqlGroup.add("SHOW " + parameter.name());
            }
            return Flux.from(SimpleQueryTask.batchAsFlux(Stmts.batch(sqlGroup), adjutant))
                    .switchIfEmpty(initializingFailure())
                    .filter(result -> result.getResultNo() >= showResultIndex)
                    .collectList()
                    .map(this::readInitializedParamResult);
        }


        /**
         * @see #connect()
         */
        private Mono<Map<String, String>> doOnInitializingSuccess(final Map<String, String> initializedParamMap) {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            if (LOG.isDebugEnabled()) {
                LOG.debug("database session initializing success,process id[{}].", adjutant.processId());
            }
            final Map<String, String> paramMap = new HashMap<>((int) (SENSITIVE_PARAM_SET.size() / 0.75F));
            for (ServerParameter parameter : SENSITIVE_PARAM_SET) {
                final String name = parameter.name();
                final String value = initializedParamMap.get(name);
                if (value != null) {
                    paramMap.put(name, value);
                }
            }
            return this.executor.handleInitializingSuccess(Collections.unmodifiableMap(paramMap))
                    .thenReturn(initializedParamMap);
        }


        /**
         * @param resultList result of  command 'SHOW xxx'
         * @return a unmodified map
         * @see #initializing()
         */
        private Map<String, String> readInitializedParamResult(final List<ResultItem> resultList) {
            final Map<String, String> map = new HashMap<>((int) (resultList.size() / 0.75F));

            for (ResultItem result : resultList) {
                if (result instanceof ResultRow) {
                    ResultRow row = (ResultRow) result;
                    ResultRowMeta rowMeta = row.getRowMeta();
                    map.put(rowMeta.getColumnLabel(0).toLowerCase(), row.get(0, String.class));
                }
            }
            if (map.isEmpty()) {
                throw new JdbdException("Session initializing failure,'SHOW xxx' execute failure.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Initialized server parameter count {} .", map.size());
            }
            return Collections.unmodifiableMap(map);
        }

        private static <T> Mono<T> initializingFailure() {
            return Mono.error(new JdbdException("Session initializing failure,SimpleQueryTask response error."));
        }


    }


}
