package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.syntax.DefaultMySQLParser;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.vendor.conf.HostInfo;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class MySQLTaskExecutor extends CommunicationTaskExecutor<MySQLTaskAdjutant> {

    static Mono<MySQLTaskExecutor> create(final int hostIndex, MySQLSessionAdjutant sessionAdjutant) {
        List<HostInfo<PropertyKey>> hostInfoList = sessionAdjutant.obtainUrl().getHostList();

        final Mono<MySQLTaskExecutor> mono;
        if (hostIndex > -1 && hostIndex < hostInfoList.size()) {
            final HostInfo<PropertyKey> hostInfo = hostInfoList.get(hostIndex);
            mono = TcpClient.create()
                    .runOn(sessionAdjutant.obtainEventLoopGroup())
                    .host(hostInfo.getHost())
                    .port(hostInfo.getPort())
                    .connect()
                    .map(connection -> new MySQLTaskExecutor(connection, hostInfo, sessionAdjutant))
            ;
        } else {
            IllegalArgumentException e = new IllegalArgumentException(
                    String.format("hostIndex[%s] not in [0,%s)", hostIndex, hostInfoList.size()));
            mono = Mono.error(new MySQLJdbdException(e, "Not found HostInfo in url."));
        }
        return mono;
    }


    static void resetTaskAdjutant(MySQLTaskExecutor taskExecutor, final Server server) {
        synchronized (taskExecutor.taskAdjutant) {
            MySQLTaskAdjutantWrapper taskAdjutant = (MySQLTaskAdjutantWrapper) taskExecutor.taskAdjutant;
            // 1.
            taskAdjutant.server = server;
            //2.
            taskAdjutant.mySQLParser = DefaultMySQLParser.create(server::containSqlMode);
            //3.
            double maxBytes = server.obtainCharsetClient().newEncoder().maxBytesPerChar();
            taskAdjutant.maxBytesPerCharClient = (int) Math.ceil(maxBytes);
        }

    }

    static void setAuthenticateResult(MySQLTaskExecutor taskExecutor, AuthenticateResult result) {
        synchronized (taskExecutor.taskAdjutant) {
            MySQLTaskAdjutantWrapper adjutantWrapper = (MySQLTaskAdjutantWrapper) taskExecutor.taskAdjutant;
            if (adjutantWrapper.handshakeV10Packet == null
                    && adjutantWrapper.negotiatedCapability == 0) {
                // 1.
                HandshakeV10Packet handshake = Objects.requireNonNull(result, "result").handshakeV10Packet();
                adjutantWrapper.handshakeV10Packet = Objects.requireNonNull(handshake, "handshake");

                //2.
                Charset serverCharset = CharsetMapping.getJavaCharsetByCollationIndex(handshake.getCollationIndex());
                if (serverCharset == null) {
                    throw new IllegalArgumentException("server handshake charset is null");
                }
                adjutantWrapper.serverHandshakeCharset = serverCharset;

                // 3.
                int negotiatedCapability = result.negotiatedCapability();
                if (negotiatedCapability == 0) {
                    throw new IllegalArgumentException("result error.");
                }
                adjutantWrapper.negotiatedCapability = negotiatedCapability;
            } else {
                throw new IllegalStateException("Duplicate update AuthenticateResult");
            }


        }
    }

    static void setCustomCollation(MySQLTaskExecutor taskExecutor, Map<Integer, CharsetMapping.CustomCollation> map) {
        synchronized (taskExecutor.taskAdjutant) {
            MySQLTaskAdjutantWrapper taskAdjutant = (MySQLTaskAdjutantWrapper) taskExecutor.taskAdjutant;
            taskAdjutant.customCollationMap = map;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskExecutor.class);

    final HostInfo<PropertyKey> hostInfo;

    final MySQLSessionAdjutant sessionAdjutant;

    private volatile int serverStatus;

    private MySQLTaskExecutor(Connection connection, HostInfo<PropertyKey> hostInfo
            , MySQLSessionAdjutant sessionAdjutant) {
        super(connection);
        this.hostInfo = hostInfo;
        this.sessionAdjutant = sessionAdjutant;
    }

    @Override
    protected Logger obtainLogger() {
        return LOG;
    }

    @Override
    protected MySQLTaskAdjutant createTaskAdjutant() {
        return new MySQLTaskAdjutantWrapper(this);
    }


    protected void updateServerStatus(Object serversStatus) {
        this.serverStatus = (Integer) serversStatus;
    }

    @Override
    protected HostInfo<?> obtainHostInfo() {
        return this.hostInfo;
    }


    /*################################## blow private method ##################################*/

    private static final class MySQLTaskAdjutantWrapper extends AbstractTaskAdjutant implements MySQLTaskAdjutant {

        private final MySQLTaskExecutor taskExecutor;

        private HandshakeV10Packet handshakeV10Packet;

        private Charset serverHandshakeCharset;

        private int negotiatedCapability = 0;

        private MySQLParser mySQLParser = DefaultMySQLParser.getForInitialization();

        private Server server;

        private int maxBytesPerCharClient = 0;

        private Map<Integer, CharsetMapping.CustomCollation> customCollationMap = Collections.emptyMap();

        private MySQLTaskAdjutantWrapper(MySQLTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
        }


        @Override
        public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
            ByteBuf packetBuffer = this.allocator().buffer(PacketUtils.HEADER_SIZE + initialPayloadCapacity);
            packetBuffer.writeZero(PacketUtils.HEADER_SIZE);
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
        public Charset obtainCharsetClient() {
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
        public int obtainNegotiatedCapability() {
            int capacity = this.negotiatedCapability;
            if (capacity == 0) {
                LOG.trace("Cannot access negotiatedCapability[{}],this[{}]", this.negotiatedCapability, this);
                throw new IllegalStateException("Cannot access negotiatedCapability now.");
            }
            return capacity;
        }

        @Override
        public Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
            return this.customCollationMap;
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
        public HandshakeV10Packet obtainHandshakeV10Packet() {
            HandshakeV10Packet packet = this.handshakeV10Packet;
            if (packet == null) {
                throw new IllegalStateException("Cannot access handshakeV10Packet now.");
            }
            return packet;
        }

        @Override
        public HostInfo<PropertyKey> obtainHostInfo() {
            return this.taskExecutor.hostInfo;
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
            return this.taskExecutor.sessionAdjutant.obtainPluginClassMap();
        }

        @Override
        public boolean isAuthenticated() {
            return this.handshakeV10Packet != null;
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

    }


}