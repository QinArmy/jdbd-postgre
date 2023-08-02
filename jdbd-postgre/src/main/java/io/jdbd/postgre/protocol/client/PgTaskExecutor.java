package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Encoding;
import io.jdbd.postgre.PgServerVersion;
import io.jdbd.postgre.Server;
import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.config.PgHost;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.env.HostInfo;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

final class PgTaskExecutor extends CommunicationTaskExecutor<TaskAdjutant> {

    static Mono<PgTaskExecutor> create(final SessionAdjutant sessionAdjutant, final int hostIndex) {
        final List<PgHost> hostList = sessionAdjutant.jdbcUrl().getHostList();

        final Mono<PgTaskExecutor> mono;
        if (hostIndex > -1 && hostIndex < hostList.size()) {
            final PgHost host = hostList.get(hostIndex);
            mono = TcpClient.create()
                    .runOn(sessionAdjutant.eventLoopGroup())
                    .host(host.getHost())
                    .port(host.getPort())
                    .connect()
                    .map(connection -> new PgTaskExecutor(connection, host, sessionAdjutant));
        } else {
            IllegalArgumentException e = new IllegalArgumentException(
                    String.format("hostIndex[%s] not in [0,%s)", hostIndex, hostList.size()));
            mono = Mono.error(new JdbdException("Not found HostInfo in url.", e));
        }
        return mono;
    }


    private static final Logger LOG = LoggerFactory.getLogger(PgTaskExecutor.class);


    private final PgHost host;

    private final SessionAdjutant sessionAdjutant;

    private PgTaskExecutor(Connection connection, PgHost host, SessionAdjutant sessionAdjutant) {
        super(connection);
        this.host = host;
        this.sessionAdjutant = sessionAdjutant;
    }

    @Override
    protected final Logger getLogger() {
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
        } else {
            String m = String.format("Unknown server status type[%s].", serverStatus.getClass().getName());
            throw new IllegalArgumentException(m);
        }

    }

    @Override
    protected final TaskAdjutant createTaskAdjutant() {
        return new TaskAdjutantWrapper(this);
    }

    @Override
    protected final HostInfo obtainHostInfo() {
        return this.host;
    }

    @Override
    protected final boolean clearChannel(ByteBuf cumulateBuffer, Class<? extends CommunicationTask> taskClass) {
        return false;
    }

    @Nullable
    @Override
    protected final void urgencyTaskIfNeed() {
        ((TaskAdjutantWrapper) this.taskAdjutant).urgencyTaskIfNeed();
    }


    final void handleAuthenticationSuccess(AuthResult result) {
        ((TaskAdjutantWrapper) this.taskAdjutant).authenticationSuccess(result);
    }


    final Mono<Void> handleInitializingSuccess(final Map<String, String> paramMap) {
        final TaskAdjutantWrapper adjutant = ((TaskAdjutantWrapper) this.taskAdjutant);
        final Mono<Void> mono;
        if (this.eventLoop.inEventLoop()) {
            adjutant.updateServerParameterStatus(paramMap);
            mono = Mono.empty();
        } else {
            mono = Mono.create(sink -> this.eventLoop.execute(() -> {
                try {
                    adjutant.updateServerParameterStatus(paramMap);
                    sink.success();
                } catch (Throwable e) {
                    sink.error(PgExceptions.wrap(e));
                }
            }));

        }
        return mono;
    }


    /*################################## blow private static class ##################################*/


    private static final class TaskAdjutantWrapper extends JdbdTaskAdjutant implements TaskAdjutant {

        private final PgTaskExecutor taskExecutor;

        private String stmtNamePrefix = "S0@";

        private String portalNamePrefix = "P0@";

        private int stmtNameId = 1;

        private int portalNameId = 1;

        private ServerImpl server;

        private TxStatus txStatus;

        private PgParser parser;

        private BackendKeyData backendKeyData;

        private List<String> urgencyParamList;

        private TaskAdjutantWrapper(PgTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
        }

        @Override
        public final PgHost obtainHost() {
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
        public final String createPrepareName() {
            final int nameId = this.stmtNameId++;
            final String prefix;
            if (nameId == Integer.MIN_VALUE) {
                final String oldPrefix = this.stmtNamePrefix;
                final int prefixNum = Integer.parseInt(oldPrefix.substring(1, oldPrefix.length() - 1)) + 1;
                prefix = oldPrefix.substring(0, 1) + prefixNum + oldPrefix.charAt(oldPrefix.length() - 1);
            } else {
                prefix = this.stmtNamePrefix;
            }
            return prefix + nameId;
        }

        @Override
        public final String createPortalName() {
            final int nameId = this.portalNameId++;
            final String prefix;
            if (nameId == Integer.MIN_VALUE) {
                final String oldPrefix = this.portalNamePrefix;
                final int prefixNum = Integer.parseInt(oldPrefix.substring(1, oldPrefix.length() - 1)) + 1;
                prefix = oldPrefix.substring(0, 1) + prefixNum + oldPrefix.charAt(oldPrefix.length() - 1);
            } else {
                prefix = this.portalNamePrefix;
            }
            return prefix + nameId;
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

        @Override
        public void appendSetCommandParameter(final String parameterName) {

            try {
                final String parameter = parameterName.toLowerCase();
                switch (ServerParameter.valueOf(parameter)) {
                    case statement_timeout:
                    case lc_monetary: {
                        addUrgencyParameter(parameter);
                    }
                    break;
                    case standard_conforming_strings: {//standard_conforming_strings was not reported by releases before 8.1
                        final ServerImpl server = Objects.requireNonNull(this.server, "this.server");
                        if (server.serverVersion.compareTo(PgServerVersion.V8_1) < 0) {
                            addUrgencyParameter(parameter);
                        }
                    }
                    break;
                    case application_name://application_name was not reported by releases before 9.0.
                    case integer_datetimes://below three were not reported by releases before 8.0
                    case server_encoding:
                    case TimeZone:
                    case IntervalStyle://IntervalStyle was not reported by releases before 8.4
                        // now, above isn't key parameter to jdbd-postgre client.
                    default:
                }
            } catch (IllegalArgumentException e) {
                // ignore,because parameterName isn't key parameter.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("non-key server parameter[{}]", parameterName);
                }
            }

        }

        /**
         * @see #appendSetCommandParameter(String)
         */
        private void addUrgencyParameter(final String parameter) {
            List<String> urgencyParamList = this.urgencyParamList;
            if (urgencyParamList == null) {
                urgencyParamList = new ArrayList<>();
                this.urgencyParamList = urgencyParamList;
            }
            urgencyParamList.add(parameter);
        }

        /**
         * @see PgTaskExecutor#urgencyTaskIfNeed()
         */
        private void urgencyTaskIfNeed() {
            final List<String> urgencyParamList = this.urgencyParamList;
            if (urgencyParamList == null || urgencyParamList.isEmpty()) {
                return;
            }
            this.urgencyParamList = null;
            final List<String> sqlList = new ArrayList<>(urgencyParamList.size());
            for (String param : urgencyParamList) {
                sqlList.add("SHOW " + param);
            }
            Flux.from(SimpleQueryTask.batchAsFlux(PgStmts.batch(sqlList), this))
                    .filter(result -> result instanceof ResultRow)
                    .cast(ResultRow.class)
                    .collectList()
                    .doOnSuccess(rowList -> {
                        if (this.inEventLoop()) {
                            updateServerParameter(rowList);
                        } else {
                            this.execute(() -> updateServerParameter(rowList));
                        }
                    }).doOnError(this::printUrgencyTaskErrorLog)
                    .subscribe();

        }

        /**
         * @see #urgencyTaskIfNeed()
         */
        private void printUrgencyTaskErrorLog(Throwable e) {
            LOG.error("UrgencyTask occur error.", e);
        }

        /**
         * @see #urgencyTaskIfNeed()
         */
        private void updateServerParameter(List<ResultRow> rowList) {
            final ServerImpl server = Objects.requireNonNull(this.server, "this.server");
            for (ResultRow row : rowList) {
                final ResultRowMeta rowMeta = row.getRowMeta();
                final String name = rowMeta.getColumnLabel(0);
                if (server.paramStatusMap.containsKey(name)) {
                    server.paramStatusMap.put(name, row.get(0, String.class));
                    LOG.debug("Trigger refresh server parameter[{}] success.", name);
                }
            }
        }

        private void authenticationSuccess(AuthResult result) {
            synchronized (this) {
                final Server server = this.server;
                if (server != null) {
                    throw new IllegalStateException("this.server is not null.");
                }
                this.server = new ServerImpl(result.serverStatusMap);
                this.backendKeyData = Objects.requireNonNull(result.backendKeyData, "result.backendKeyData");
                this.txStatus = Objects.requireNonNull(result.txStatus, "txStatus");
                this.parser = PgParser.create(this.server::parameter);

                if (LOG.isDebugEnabled()) {
                    printAuthResultStatuses(result);
                }

            }

        }

        private void updateServerParameterStatus(Map<String, String> paramStatusMap) {
            if (!this.taskExecutor.eventLoop.inEventLoop()) {
                throw new IllegalStateException("Not in netty EventLoop.");
            }
            final ServerImpl server = Objects.requireNonNull(this.server, "this.server");
            server.updateServerParams(paramStatusMap);
        }

        private void updateTxStatus(TxStatus txStatus) {
            this.txStatus = txStatus;
        }

        private static void printAuthResultStatuses(AuthResult result) {
            final String line = System.lineSeparator();
            StringBuilder builder = new StringBuilder();

            int count = 0;
            for (Map.Entry<String, String> e : result.serverStatusMap.entrySet()) {
                if (count > 0) {
                    builder.append(",")
                            .append(line);
                }
                builder.append(e.getKey())
                        .append("=")
                        .append(e.getValue());
                count++;
            }
            builder.append(line)
                    .append("}");

            LOG.debug("Server[process id:{}] parameter statuses:{{}{}", result.backendKeyData.processId, line, builder);
        }


    }

    private static final class ServerImpl implements Server {

        private static final Map<String, Locale> LOCALE_MAP = createLocalMap();

        private final PgServerVersion serverVersion;

        // non-volatile ,because run in netty EventLoop.
        private final Map<String, String> paramStatusMap;

        private ServerImpl(final Map<String, String> paramStatusMap) {
            this.paramStatusMap = new HashMap<>(paramStatusMap);
            this.serverVersion = PgServerVersion.from(paramStatusMap.get(ServerParameter.server_version.name()));
        }


        public void updateServerParams(final Map<String, String> paramStatusMap) {
            this.paramStatusMap.putAll(paramStatusMap);
        }

        @Override
        public final PgServerVersion serverVersion() {
            return this.serverVersion;
        }

        @Override
        public final String parameter(ServerParameter parameter) {
            return this.paramStatusMap.get(parameter.name());
        }

        @Override
        public final ZoneOffset zoneOffset() {
            final ZoneId zoneId = ZoneId.of(paramStatusMap.get(ServerParameter.TimeZone.name()), ZoneId.SHORT_IDS);
            return PgTimes.toZoneOffset(zoneId);
        }

        @Override
        public final IntervalStyle intervalStyle() {
            try {
                return IntervalStyle.valueOf(this.paramStatusMap.get(ServerParameter.IntervalStyle.name()));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public final Locale moneyLocal() {
            final String value = this.paramStatusMap.get(ServerParameter.lc_monetary.name());
            if (value == null) {
                throw new IllegalStateException(String.format("Not found server parameter[%s]."
                        , ServerParameter.lc_monetary));
            }
            final int index = value.indexOf('.');
            final String tag = index < 0 ? value : value.substring(0, index);
            Locale locale = LOCALE_MAP.get(tag);
            if (locale == null) {
                final String[] slice = tag.split("_");
                if (slice.length != 2) {
                    throw moneyLocalFormatError(value);
                }
                try {
                    locale = new Locale(slice[0], slice[1]);
                } catch (RuntimeException e) {
                    throw moneyLocalFormatError(value);
                }

            }
            return locale;
        }

        private static IllegalStateException moneyLocalFormatError(String value) {
            return new IllegalStateException(String.format("Not found money local for %s .", value));
        }

        private static Map<String, Locale> createLocalMap() {
            final Locale[] locales = Locale.getAvailableLocales();
            final Map<String, Locale> map = new HashMap<>((int) (locales.length / 0.75F));
            for (Locale locale : locales) {
                if (PgStrings.hasText(locale.getCountry())) {
                    map.put(locale.toString(), locale);
                }
            }
            return Collections.unmodifiableMap(map);
        }


    }


}
