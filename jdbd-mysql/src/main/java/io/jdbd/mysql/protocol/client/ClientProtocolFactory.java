package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLArrays;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.env.Properties;
import io.jdbd.vendor.util.Pair;
import io.jdbd.vendor.util.SQLStates;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpResources;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.*;

public final class ClientProtocolFactory extends FixedEnv implements MySQLProtocolFactory {

    final MySQLHost host;

    final int factoryTaskQueueSize;


    final TcpClient tcpClient;

    private ClientProtocolFactory(final MySQLHost host) {
        super(host.environment());
        this.host = host;


        this.factoryTaskQueueSize = this.env.getInRange(MySQLKey.FACTORY_TASK_QUEUE_SIZE, 3, 4096);


        this.tcpClient = TcpClient.create(this.env.get(MySQLKey.SOCKET_FACTORY, TcpResources::get))
                .runOn(createEventLoopGroup(this.env))
                .host(host.getHost())
                .port(host.getPort());
    }


    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolFactory.class);

    public static ClientProtocolFactory from(Environment env) {
        throw new UnsupportedOperationException();
    }


    public static Mono<ClientProtocol0> single(final SessionAdjutant sessionAdjutant) {
        return MySQLTaskExecutor.create(0, sessionAdjutant)//1. create tcp connection
                .map(executor -> new SessionManagerImpl(executor, sessionAdjutant, 0))//2. create  SessionManagerImpl
                .flatMap(SessionManagerImpl::authenticate) //3. authenticate
                .flatMap(SessionManagerImpl::initializing)//4. initializing
                .flatMap(ProtocolManager::reset)           //5. reset
                .map(ClientProtocol::create);         //6. create ClientProtocol
    }


    @Override
    public Mono<MySQLProtocol> createProtocol() {
        return null;
    }

    @Override
    public Mono<Void> close() {
        return null;
    }


    private static EventLoopGroup createEventLoopGroup(final Environment env) {
        return LoopResources.create("jdbd-mysql", env.getOrDefault(MySQLKey.FACTORY_WORKER_COUNT), true)
                .onClient(true);
    }


    private static final class SessionManagerImpl implements ProtocolManager {

        private static final List<String> KEY_VARIABLES = MySQLArrays.asUnmodifiableList(
                "sql_mode",
                "time_zone",
                "transaction_isolation",
                "transaction_read_only",
                "autocommit"
        );

        private final MySQLTaskExecutor executor;

        private final SessionAdjutant sessionAdjutant;

        private final int hostIndex;

        private SessionManagerImpl(MySQLTaskExecutor executor, SessionAdjutant sessionAdjutant, int hostIndex) {
            this.executor = executor;
            this.sessionAdjutant = sessionAdjutant;
            this.hostIndex = hostIndex;
        }

        @Override
        public TaskAdjutant adjutant() {
            return this.executor.taskAdjutant();
        }

        @Override
        public Mono<ProtocolManager> reset() {

            final Pair<Charset, String> connClientPair;
            final Pair<Charset, String> resultsCharsetPair;
            final String setCustomVariablesSql;
            try {
                setCustomVariablesSql = createSetVariablesSql();
                connClientPair = getConnClientPair();
                resultsCharsetPair = getResultsCharsetPair();
            } catch (Throwable e) {
                return Mono.error(e);
            }
            final Charset clientCharset = connClientPair.getFirst();
            final Charset resultCharset = resultsCharsetPair.getFirst();

            final List<String> sqlGroup = MySQLCollections.arrayList(8);
            sqlGroup.add(setCustomVariablesSql); //1.
            sqlGroup.add(connClientPair.getSecond());//2.
            sqlGroup.add(resultsCharsetPair.getSecond());//3.
            sqlGroup.add(createKeyVariablesSql());//4.
            sqlGroup.add("SELECT @@SESSION.sql_mode");//5.

            final MultiResult result;
            result = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), this.executor.taskAdjutant());
            return Mono.from(result.nextUpdate())//1. SET custom variables
                    .then(Mono.from(result.nextUpdate()))//2. SET character_set_connection and character_set_client
                    .then(Mono.from(result.nextUpdate()))//3.SET character_set_results
                    .then(Mono.from(result.nextUpdate()))//4.SET key variables
                    .thenMany(result.nextQuery())//5. SELECT sql_mode
                    .last()
                    .map(row -> new DefaultServer(clientCharset, resultCharset, row.getNonNull(0, String.class)))
                    .doOnSuccess(this.executor::resetTaskAdjutant)

                    .switchIfEmpty(Mono.defer(this::resetFailure))
                    .thenReturn(this);
        }


        @Override
        public Mono<Void> reConnect() {
            return this.executor.reConnect()
                    .then(Mono.defer(this::authenticate))
                    .then(Mono.defer(this::initializing))
                    .then(Mono.defer(this::reset))
                    .then();
        }


        private Mono<SessionManagerImpl> authenticate() {
            return MySQLConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(this.executor::setAuthenticateResult)
                    .thenReturn(this);
        }


        private Mono<SessionManagerImpl> initializing() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.mysqlUrl().getCommonProps();

            if (!properties.getOrDefault(MyKey.detectCustomCollations, Boolean.class)) {
                return Mono.just(this);
            }
            final List<String> sqlGroup = new ArrayList<>(3);
            sqlGroup.add("SHOW CHARACTER SET");
            sqlGroup.add("SHOW COLLATION");
            final MultiResult result = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), adjutant);
            return Flux.from(result.nextQuery())// SHOW CHARACTER SET result
                    .filter(this::isCustomCharset)
                    .map(this::createCustomCharset)
                    .collectMap(MyCharset::charsetName, MyCharset::self)
                    .map(Collections::unmodifiableMap)

                    .flatMap(customCharsetMap -> handleCustomCollation(result, customCharsetMap))// handle SHOW COLLATION result
                    .thenReturn(this)
                    ;
        }

        /**
         * @see #initializing()
         */
        private Mono<Void> handleCustomCollation(final MultiResult result
                , final Map<String, MyCharset> customCharsetMap) {
            return Flux.from(result.nextQuery())
                    .filter(row -> isCustomCollation(row, customCharsetMap))
                    .map(row -> createCustomCollation(row, customCharsetMap))
                    .collectMap(Collation::index, Collation::self)
                    .map(Collections::unmodifiableMap)
                    .flatMap(customCollationMap -> this.executor.setCustomCollation(customCharsetMap, customCollationMap))
                    .then();
        }

        private boolean isCustomCharset(final ResultRow row) {
            final String name = row.getNonNull("Charset", String.class);
            return !Charsets.NAME_TO_CHARSET.containsKey(name)
                    && adjutant().customCharsetMap().containsKey(name)// custom charset can map to java charset.
                    ;
        }

        private MyCharset createCustomCharset(final ResultRow row) {
            final String name = row.getNonNull("Charset", String.class);
            final Charset charset = adjutant().customCharsetMap().get(name);
            return new MyCharset(name, row.getNonNull("Maxlen", Integer.class), 0, charset.name());
        }

        private boolean isCustomCollation(final ResultRow row, final Map<String, MyCharset> customCharsetMap) {
            return !Charsets.INDEX_TO_COLLATION.containsKey(row.getNonNull("Id", Integer.class))
                    && customCharsetMap.containsKey(row.getNonNull("Charset", String.class));
        }

        private Collation createCustomCollation(final ResultRow row, final Map<String, MyCharset> customCharsetMap) {
            final int id = row.getNonNull("Id", Integer.class);
            final String collationName = row.getNonNull("Collation", String.class);
            final String charsetName = row.getNonNull("Charset", String.class);
            MyCharset myCharset = Charsets.NAME_TO_CHARSET.get(charsetName);
            if (myCharset == null) {
                myCharset = customCharsetMap.get(charsetName);
            }
            return new Collation(id, collationName, 0, myCharset);
        }

        private Pair<Charset, String> getResultsCharsetPair() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.host().getProperties();
            final String charsetString = properties.get(MyKey.characterSetResults, String.class);

            final Charset charsetResults;
            final String command;
            if (charsetString == null
                    || charsetString.equalsIgnoreCase(Constants.NULL)) {
                command = "SET character_set_results = NULL";
                charsetResults = StandardCharsets.ISO_8859_1;
            } else if (charsetString.equalsIgnoreCase("binary")) {
                command = "SET character_set_results = 'binary'";
                charsetResults = StandardCharsets.ISO_8859_1;
            } else {
                MyCharset myCharset;
                myCharset = Charsets.NAME_TO_CHARSET.get(charsetString.toLowerCase());
                if (myCharset == null) {
                    myCharset = adjutant.nameCharsetMap().get(charsetString.toLowerCase());
                }
                if (myCharset == null) {
                    String message = String.format("No found MySQL charset[%s] fro Property[%s]"
                            , charsetString, MyKey.characterSetResults);
                    throw new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION));
                }
                command = String.format("SET character_set_results = '%s'", myCharset.name);
                charsetResults = Charset.forName(myCharset.javaEncodingsUcList.get(0));
            }
            return new Pair<>(charsetResults, command);
        }

        private String createSetVariablesSql() throws MySQLJdbdException {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.host().getProperties();
            final String pairString = properties.get(MyKey.sessionVariables);
            final String sql;
            if (MySQLStrings.hasText(pairString)) {
                sql = Commands.buildSetVariableCommand(pairString);
            } else {
                sql = "SET @@session.character_set_results = DEFAULT";
            }
            return sql;
        }

        private Pair<Charset, String> getConnClientPair() {
            final Collation connCollation;
            connCollation = getConnCollation();

            final String command;
            final Charset charset;
            if (connCollation == null) {
                final Pair<MyCharset, Charset> charsetPair = getClientMyCharsetPair();
                final MyCharset myCharset = charsetPair.getFirst();
                charset = charsetPair.getSecond();
                final String format = "SET character_set_client = '%s',character_set_connection = '%s'";
                command = String.format(format, myCharset.name, myCharset.name);
            } else {
                final String format;
                format = "SET character_set_client = '%s' COLLATE '%s',character_set_connection = '%s' COLLATE '%s'";
                charset = Charset.forName(connCollation.myCharset.javaEncodingsUcList.get(0));
                command = String.format(format
                        , connCollation.myCharset.name
                        , connCollation.name
                        , connCollation.myCharset.name
                        , connCollation.name);
            }
            return new Pair<>(charset, command);
        }

        /**
         * @see #getConnClientPair()
         */
        @Nullable
        private Collation getConnCollation() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.host().getProperties();
            final String collationStr = properties.get(MyKey.connectionCollation, String.class);

            final Collation collation;
            if (MySQLStrings.hasText(collationStr)) {
                Collation tempCollation = Charsets.getCollationByName(collationStr);
                if (tempCollation == null) {
                    tempCollation = adjutant.nameCollationMap().get(collationStr.toLowerCase());
                }
                if (tempCollation == null) {
                    String message = String.format("No found MySQL Collation[%s] fro Property[%s]"
                            , collationStr, MyKey.connectionCollation);
                    throw new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION));
                }
                Charset charset = Charset.forName(tempCollation.myCharset.javaEncodingsUcList.get(0));
                if (Charsets.isSupportCharsetClient(charset)) {
                    collation = tempCollation;
                } else {
                    collation = null;
                }
            } else {
                collation = null;
            }
            return collation;
        }

        /**
         * @see #getConnClientPair()
         */
        private Pair<MyCharset, Charset> getClientMyCharsetPair() {
            final TaskAdjutant adjutant = this.executor.taskAdjutant();
            final Properties properties = adjutant.host().getProperties();
            Charset charset = properties.get(MyKey.characterEncoding, Charset.class);

            MyCharset myCharset;
            if (charset == null || !Charsets.isSupportCharsetClient(charset)) {
                charset = StandardCharsets.UTF_8;
                myCharset = Charsets.NAME_TO_CHARSET.get(Charsets.utf8mb4);
            } else {
                final MySQLServerVersion version = adjutant.handshake10().getServerVersion();
                myCharset = Charsets.getMysqlCharsetForJavaEncoding(charset.name(), version);
                if (myCharset == null) {
                    charset = StandardCharsets.UTF_8;
                    myCharset = Charsets.NAME_TO_CHARSET.get(Charsets.utf8mb4);
                }
            }
            return new Pair<>(myCharset, charset);
        }

        /**
         * @see #reset()
         */
        private <T> Mono<T> resetFailure() {
            // not bug ,never here.
            return Mono.error(new JdbdSQLException(new SQLException("reset failure,no any result")));
        }

        private String createKeyVariablesSql() {
            final StringBuilder builder = new StringBuilder();
            builder.append("SET ");
            final int size = KEY_VARIABLES.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append("@@SESSION.")
                        .append(KEY_VARIABLES.get(i))
                        .append(" = DEFAULT");
            }
            return builder.toString();
        }


    }

    private static final class DefaultServer implements Server {

        private final Charset clientCharset;

        private final Charset resultsCharset;

        private final Set<String> sqlModeSet;

        private DefaultServer(Charset clientCharset, @Nullable Charset resultsCharset, String sqlMode) {
            this.clientCharset = Objects.requireNonNull(clientCharset, "clientCharset");
            this.resultsCharset = StandardCharsets.ISO_8859_1.equals(resultsCharset) ? null : resultsCharset;
            this.sqlModeSet = Collections.unmodifiableSet(MySQLStrings.spitAsSet(sqlMode, ","));
        }

        @Override
        public boolean containSqlMode(final SQLMode sqlMode) {
            return this.sqlModeSet.contains(sqlMode.name());
        }

        @Override
        public Charset obtainCharsetClient() {
            return this.clientCharset;
        }

        @Override
        public Charset obtainCharsetResults() {
            return this.resultsCharset;
        }

        @Override
        public ZoneOffset obtainZoneOffsetDatabase() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZoneOffset obtainZoneOffsetClient() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportLocalInfile() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return new StringBuilder("DefaultServer{")
                    .append("\nclientCharset=").append(clientCharset)
                    .append("\n, resultsCharset=").append(resultsCharset)
                    .append("\n, sqlModeSet=").append(sqlModeSet)
                    .append("\n}")
                    .toString();
        }


    }


}
