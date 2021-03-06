package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.syntax.DefaultMySQLParser;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

final class MySQLTaskExecutor extends CommunicationTaskExecutor<MySQLTaskAdjutant> {

    static Mono<MySQLTaskExecutor> create(HostInfo<PropertyKey> hostInfo, EventLoopGroup eventLoopGroup) {
        return TcpClient.create()
                .runOn(eventLoopGroup)
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .connect()
                .map(connection -> new MySQLTaskExecutor(connection, hostInfo))
                ;
    }


    static void resetTaskAdjutant(MySQLTaskExecutor taskExecutor, final Server server) {
        synchronized (taskExecutor.taskAdjutant) {
            MySQLTaskAdjutantWrapper taskAdjutant = (MySQLTaskAdjutantWrapper) taskExecutor.taskAdjutant;
            // 1.
            taskAdjutant.server = server;
            //2.
            taskAdjutant.mySQLParser = DefaultMySQLParser.create(server);
            //3.
            double maxBytes = server.obtainCharsetClient().newEncoder().maxBytesPerChar();
            taskAdjutant.maxBytesPerCharClient = (int) Math.ceil(maxBytes);
        }

    }

    static void setAuthenticateResult(MySQLTaskExecutor taskExecutor, AuthenticateResult result) {
        synchronized (taskExecutor.taskAdjutant) {
            MySQLTaskAdjutantWrapper taskAdjutant = (MySQLTaskAdjutantWrapper) taskExecutor.taskAdjutant;
            if (taskAdjutant.handshakeV10Packet == null
                    && taskAdjutant.negotiatedCapability == 0) {
                // 1.
                HandshakeV10Packet handshake = Objects.requireNonNull(result.handshakeV10Packet()
                        , "result.handshakeV10Packet()");
                taskAdjutant.handshakeV10Packet = handshake;

                //2.
                Charset serverCharset = CharsetMapping.getJavaCharsetByCollationIndex(handshake.getCollationIndex());
                if (serverCharset == null) {
                    throw new IllegalArgumentException("server handshake charset is null");
                }
                taskAdjutant.serverHandshakeCharset = serverCharset;

                // 3.
                int negotiatedCapability = result.negotiatedCapability();
                if (negotiatedCapability == 0) {
                    throw new IllegalArgumentException("result error.");
                }
                taskAdjutant.negotiatedCapability = negotiatedCapability;

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


    final HostInfo<PropertyKey> hostInfo;

    private volatile int serverStatus;

    private MySQLTaskExecutor(Connection connection, HostInfo<PropertyKey> hostInfo) {
        super(connection);
        this.hostInfo = hostInfo;
    }

    @Override
    protected MySQLTaskAdjutant createTaskAdjutant() {
        return new MySQLTaskAdjutantWrapper(this);
    }


    protected void updateServerStatus(Object serversStatus) {
        this.serverStatus = (Integer) serversStatus;
    }




    /*################################## blow private method ##################################*/

    private static final class MySQLTaskAdjutantWrapper extends AbstractTaskAdjutant implements MySQLTaskAdjutant {

        private final MySQLTaskExecutor taskExecutor;

        private final HostInfo<PropertyKey> hostInfo;

        private HandshakeV10Packet handshakeV10Packet;

        private Charset serverHandshakeCharset;

        private int negotiatedCapability = 0;

        private MySQLParser mySQLParser;

        private Server server;

        private int maxBytesPerCharClient = 0;

        private Map<Integer, CharsetMapping.CustomCollation> customCollationMap = Collections.emptyMap();

        private MySQLTaskAdjutantWrapper(MySQLTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
            this.hostInfo = taskExecutor.hostInfo;
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
            return this.hostInfo;
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
