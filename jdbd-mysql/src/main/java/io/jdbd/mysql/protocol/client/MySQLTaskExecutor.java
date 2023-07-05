package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.MySQLHost;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.syntax.DefaultMySQLParser;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.vendor.env.HostInfo;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.*;

final class MySQLTaskExecutor extends CommunicationTaskExecutor<TaskAdjutant> {

    static Mono<MySQLTaskExecutor> create(final int hostIndex, SessionAdjutant sessionAdjutant) {
        List<MySQLHost> hostInfoList = sessionAdjutant.jdbcUrl().getHostList();

        final Mono<MySQLTaskExecutor> mono;
        if (hostIndex > -1 && hostIndex < hostInfoList.size()) {
            final MySQLHost hostInfo = hostInfoList.get(hostIndex);
            mono = TcpClient.create()
                    .runOn(sessionAdjutant.eventLoopGroup())
                    .host(hostInfo.getHost())
                    .port(hostInfo.getPort())
                    .connect()
                    .map(connection -> new MySQLTaskExecutor(connection, hostInfo, sessionAdjutant));
        } else {
            String m = String.format("hostIndex[%s] not in [0,%s)", hostIndex, hostInfoList.size());
            mono = Mono.error(new JdbdException("Not found HostInfo in url.", new IllegalArgumentException(m)));
        }
        return mono;
    }


    @Deprecated
    static void setCustomCollation(MySQLTaskExecutor taskExecutor, Map<Integer, Charsets.CustomCollation> map) {

    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskExecutor.class);

    final MySQLHost hostInfo;

    final SessionAdjutant sessionAdjutant;

    private volatile int serverStatus;

    private MySQLTaskExecutor(Connection connection, MySQLHost hostInfo
            , SessionAdjutant sessionAdjutant) {
        super(connection);
        this.hostInfo = hostInfo;
        this.sessionAdjutant = sessionAdjutant;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected TaskAdjutant createTaskAdjutant() {
        return new TaskAdjutantWrapper(this);
    }


    protected void updateServerStatus(Object serversStatus) {
        // LOG.debug("serversStatus :{}", serversStatus);
    }

    @Override
    protected HostInfo obtainHostInfo() {
        return this.hostInfo;
    }

    @Override
    protected boolean clearChannel(ByteBuf cumulateBuffer, Class<? extends CommunicationTask> taskClass) {
        //TODO zoro complement this method.
        return true;
    }

    void setAuthenticateResult(AuthenticateResult result) {
        synchronized (this.taskAdjutant) {
            TaskAdjutantWrapper adjutantWrapper = (TaskAdjutantWrapper) this.taskAdjutant;
            if (adjutantWrapper.handshake10 == null
                    && adjutantWrapper.negotiatedCapability == 0) {
                // 1.
                Handshake10 handshake = Objects.requireNonNull(result, "result").handshakeV10Packet();
                adjutantWrapper.handshake10 = Objects.requireNonNull(handshake, "handshake");

                //2.
                Charset serverCharset = Charsets.getJavaCharsetByCollationIndex(handshake.getCollationIndex());
                if (serverCharset == null) {
                    throw new IllegalArgumentException("server handshake charset is null");
                }
                adjutantWrapper.serverHandshakeCharset = serverCharset;

                // 3.
                int negotiatedCapability = result.capability();
                if (negotiatedCapability == 0) {
                    throw new IllegalArgumentException("result error.");
                }
                adjutantWrapper.negotiatedCapability = negotiatedCapability;
            } else {
                throw new IllegalStateException("Duplicate update AuthenticateResult");
            }


        }
    }

    void resetTaskAdjutant(final Server server) {
        LOG.debug("reset success,server:{}", server);
        synchronized (this.taskAdjutant) {
            TaskAdjutantWrapper taskAdjutant = (TaskAdjutantWrapper) this.taskAdjutant;
            // 1.
            taskAdjutant.server = server;
            //2.
            taskAdjutant.mySQLParser = DefaultMySQLParser.create(server::containSqlMode);
            //3.
            double maxBytes = server.obtainCharsetClient().newEncoder().maxBytesPerChar();
            taskAdjutant.maxBytesPerCharClient = (int) Math.ceil(maxBytes);
        }

    }

    Mono<Void> reConnect() {
        return Mono.empty();
    }


    Mono<Void> setCustomCollation(final Map<String, MyCharset> customCharsetMap
            , final Map<Integer, Collation> customCollationMap) {
        final Mono<Void> mono;
        final TaskAdjutantWrapper adjutant = ((TaskAdjutantWrapper) this.taskAdjutant);
        if (this.eventLoop.inEventLoop()) {
            adjutant.setCustomCharsetMap(customCharsetMap);
            adjutant.setIdCollationMap(customCollationMap);
            mono = Mono.empty();
        } else {
            mono = Mono.create(sink -> this.eventLoop.execute(() -> {
                adjutant.setCustomCharsetMap(customCharsetMap);
                adjutant.setIdCollationMap(customCollationMap);
                sink.success();
            }));
        }
        return mono;
    }


    /*################################## blow private method ##################################*/

    private static final class TaskAdjutantWrapper extends AbstractTaskAdjutant implements TaskAdjutant {

        private final MySQLTaskExecutor taskExecutor;

        private Handshake10 handshake10;

        private Charset serverHandshakeCharset;

        private int negotiatedCapability = 0;

        private MySQLParser mySQLParser = DefaultMySQLParser.getForInitialization();

        private Server server;

        private int maxBytesPerCharClient = 0;

        private Map<String, MyCharset> customCharsetMap = Collections.emptyMap();

        private Map<Integer, Collation> idCollationMap = Collections.emptyMap();

        private Map<String, Collation> nameCollationMap = Collections.emptyMap();

        private TaskAdjutantWrapper(MySQLTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
        }


        @Override
        public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
            ByteBuf packetBuffer = this.allocator().buffer(Packets.HEADER_SIZE + initialPayloadCapacity, 1 << 30);
            packetBuffer.writeZero(Packets.HEADER_SIZE);
            return packetBuffer;

        }

        @Override
        public ByteBuf createPacketBuffer(int initialPayloadCapacity, int maxCapacity) {
            ByteBuf packetBuffer = this.allocator().buffer(Packets.HEADER_SIZE + initialPayloadCapacity
                    , maxCapacity);
            packetBuffer.writeZero(Packets.HEADER_SIZE);
            return packetBuffer;
        }

        @Override
        public int obtainMaxBytesPerCharClient() {
            int maxBytes = this.maxBytesPerCharClient;
            if (maxBytes < 1) {
                throw new IllegalStateException("Cannot access maxBytesPerCharClient now.");
            }
            return maxBytes;
        }

        @Override
        public Charset charsetClient() {
            Server server = this.server;
            return server == null ? StandardCharsets.UTF_8 : server.obtainCharsetClient();
        }

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-client-configuration">Client Program Connection Character Set Configuration</a>
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_character_set_results">character_set_results</a>
         */
        @Nullable
        @Override
        public Charset getCharsetResults() {
            Server server = this.server;
            Charset charset;
            if (server == null) {
                charset = StandardCharsets.UTF_8;
            } else {
                charset = server.obtainCharsetResults();
            }
            return charset;
        }

        @Override
        public Charset obtainColumnCharset(Charset columnCharset) {
            Charset charset = getCharsetResults();
            if (charset == null) {
                charset = columnCharset;
            }
            return charset;
        }

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-errors.html">Error Message Character Set</a>
         */
        @Override
        public Charset obtainCharsetError() {
            Charset errorCharset = getCharsetResults();
            if (errorCharset == null) {
                errorCharset = StandardCharsets.UTF_8;
            }
            return errorCharset;
        }

        @Override
        public Charset obtainCharsetMeta() {
            Charset metaCharset = getCharsetResults();
            if (metaCharset == null) {
                metaCharset = StandardCharsets.UTF_8;
            }
            return metaCharset;
        }

        @Override
        public int capability() {
            int capacity = this.negotiatedCapability;
            if (capacity == 0) {
                LOG.trace("Cannot access negotiatedCapability[{}],this[{}]", this.negotiatedCapability, this);
                throw new IllegalStateException("Cannot access negotiatedCapability now.");
            }
            return capacity;
        }

        @Override
        public Map<Integer, Charsets.CustomCollation> obtainCustomCollationMap() {
            return Collections.emptyMap();
        }

        @Override
        public ZoneOffset obtainZoneOffsetDatabase() {
            Server server = this.server;
            if (server == null) {
                throw new IllegalStateException("Cannot access zoneOffsetDatabase now.");
            }
            return server.obtainZoneOffsetDatabase();
        }

        @Override
        public Handshake10 handshake10() {
            Handshake10 packet = this.handshake10;
            if (packet == null) {
                throw new IllegalStateException("Cannot access handshakeV10Packet now.");
            }
            return packet;
        }

        @Override
        public MySQLHost host() {
            return this.taskExecutor.hostInfo;
        }

        @Override
        public MySQLUrl mysqlUrl() {
            return this.taskExecutor.sessionAdjutant.jdbcUrl();
        }

        @Override
        public ZoneOffset obtainZoneOffsetClient() {
            Server server = this.server;
            if (server == null) {
                throw new IllegalStateException("Cannot access zoneOffsetClient now.");
            }
            return server.obtainZoneOffsetClient();
        }

        @Override
        public int getServerStatus() throws IllegalStateException {
            return this.taskExecutor.serverStatus;
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> obtainPluginMechanismMap() {
            return this.taskExecutor.sessionAdjutant.pluginClassMap();
        }

        @Override
        public boolean isAuthenticated() {
            return this.handshake10 != null;
        }

        @Override
        public MySQLParser sqlParser() {
            final MySQLParser parser = this.mySQLParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access mySQLParser now.");
            }
            return parser;
        }

        @Override
        public Map<String, Charset> customCharsetMap() {
            return this.taskExecutor.sessionAdjutant.customCharsetMap();
        }

        @Override
        public Map<String, MyCharset> nameCharsetMap() {
            final Map<String, MyCharset> map = this.customCharsetMap;
            if (map == null) {
                throw new IllegalStateException("this.customCharsetMap is null.");
            }
            return map;
        }

        @Override
        public Map<Integer, Collation> idCollationMap() {
            final Map<Integer, Collation> map = this.idCollationMap;
            if (map == null) {
                throw new IllegalStateException("this.customCollationMap is null.");
            }
            return map;
        }

        @Override
        public Map<String, Collation> nameCollationMap() {
            final Map<String, Collation> map = this.nameCollationMap;
            if (map == null) {
                throw new IllegalStateException("this.nameCollationMap is null.");
            }
            return map;
        }

        @Override
        public boolean inTransaction() {
            return false;
        }

        @Override
        public boolean isAutoCommit() {
            return false;
        }

        @Override
        public Server obtainServer() {
            Server server = this.server;
            if (server == null) {
                throw new IllegalStateException("Cannot access server now.");
            }
            return server;
        }


        @Override
        public MySQLStatement parse(String singleSql) throws SQLException {
            MySQLParser parser = this.mySQLParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.parse(singleSql);
        }

        @Override
        public boolean isSingleStmt(String sql) throws SQLException {
            MySQLParser parser = this.mySQLParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.isSingleStmt(sql);
        }

        @Override
        public boolean isMultiStmt(String sql) throws SQLException {
            MySQLParser parser = this.mySQLParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.isMultiStmt(sql);
        }


        private void setCustomCharsetMap(Map<String, MyCharset> customCharsetMap) {
            this.customCharsetMap = customCharsetMap;
        }

        private void setIdCollationMap(final Map<Integer, Collation> idCollationMap) {
            final Map<String, Collation> nameCollationMap = new HashMap<>((int) (idCollationMap.size() / 0.75F));
            for (Collation collation : idCollationMap.values()) {
                nameCollationMap.put(collation.name, collation);
            }
            this.nameCollationMap = nameCollationMap;
            this.idCollationMap = idCollationMap;
        }


    }


}
