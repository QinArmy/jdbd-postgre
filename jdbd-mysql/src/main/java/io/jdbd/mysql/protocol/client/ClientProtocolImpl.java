package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.StringUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.jdbd.mysql.protocol.conf.PropertyDefinitions.SslMode;

public final class ClientProtocolImpl implements ClientProtocol, ProtocolAssistant {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolImpl.class);

    private static final String NONE = "none";

    public static Mono<ClientProtocol> getInstance(MySQLUrl mySQLUrl) {
        if (mySQLUrl.getProtocol() != MySQLUrl.Protocol.SINGLE_CONNECTION) {
            throw new IllegalArgumentException(
                    String.format("mySQLUrl protocol isn't %s", MySQLUrl.Protocol.SINGLE_CONNECTION));
        }
        HostInfo hostInfo = mySQLUrl.getHosts().get(0);
        return TcpClient.create()
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .connect()
                .map(connection -> new ClientProtocolImpl(mySQLUrl, connection))
                ;
    }


    private final MySQLUrl mySQLUrl;

    private final Connection connection;

    private final Properties properties;

    private final AtomicReference<Byte> clientCollationIndex = new AtomicReference<>(null);

    private final AtomicReference<AbstractHandshakePacket> handshakePacket = new AtomicReference<>(null);

    private final Charset clientCharset;

    private final boolean useSsl;

    private ClientProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();
        this.clientCharset = Charset.forName(this.properties.getRequiredProperty(PropertyKey.characterEncoding));

        this.useSsl = this.properties.getRequiredProperty(PropertyKey.useSSL, Boolean.class);
    }


    @Override
    public final Mono<MySQLPacket> handshake() {
        final AtomicInteger payloadLength = new AtomicInteger(-1);
        final AtomicInteger payloadCount = new AtomicInteger(0);
        return this.connection.inbound()
                .receive()
                .bufferUntil(byteBuf -> {
                    if (payloadLength.get() < 0) {
                        if (ErrorPacket.isErrorPacket(byteBuf)) {
                            return true;
                        }
                        payloadLength.set(PacketUtils.getInt3(byteBuf, byteBuf.readerIndex()));
                    }
                    return payloadCount.addAndGet(byteBuf.readableBytes()) >= payloadLength.get();
                }).map(ByteBufferUtils::mergeByteBuf)
                .elementAt(0)
                .map(this::parseHandshakePacket)
                .flatMap(this::handleHandshakePacket);
    }


    @Override
    public final Mono<MySQLPacket> responseHandshake() {
        AbstractHandshakePacket packet = this.handshakePacket.get();
        if (packet == null) {
            return Mono.error(new JdbdRuntimeException("ClientProtocol no handshake.") {
            });
        }
        Mono<MySQLPacket> mono;
        if (packet instanceof HandshakeV10Packet) {
            HandshakeV10Packet handshakeV10Packet = (HandshakeV10Packet) packet;
            if ((handshakeV10Packet.getCapabilityFlags() & ClientProtocol.CLIENT_PROTOCOL_41) != 0) {
                mono = writeHandshakeResponse41();
            } else {
                mono = writeHandshakeResponse320();
            }
        } else {
            mono = writeHandshakeResponse320();
        }
        return mono;
    }

    @Override
    public final Mono<MySQLPacket> sslRequest() {
        return Mono.empty();
    }

    /*################################## blow ProtocolAssistant method ##################################*/

    @Override
    public Charset getClientCharset() {
        return this.clientCharset;
    }

    @Override
    public Charset getPasswordCharset() {
        String pwdCharset = this.properties.getProperty(PropertyKey.passwordCharacterEncoding);
        return pwdCharset == null ? this.clientCharset : Charset.forName(pwdCharset);
    }

    @Override
    public HostInfo getMainHostInfo() {
        return this.mySQLUrl.getHosts().get(0);
    }

    @Override
    public boolean isUseSsl() {
        return this.useSsl;
    }

    @Override
    public ByteBuf createPacketBuffer(int payloadCapacity) {
        return PacketUtils.createPacketBuffer(this.connection, payloadCapacity);
    }

    @Override
    public ByteBuf createEmptyPacketForWrite() {
        return PacketUtils.createEmptyPacket(this.connection);
    }


    /*################################## blow private method ##################################*/


    private MySQLPacket parseHandshakePacket(ByteBuf byteBuf) {

        if (ErrorPacket.isErrorPacket(byteBuf)) {
            return ErrorPacket.readPacket(byteBuf);
        }
        MySQLPacket packet;
        short version = PacketUtils.getInt1(byteBuf, MySQLPacket.HEAD_LENGTH);
        switch (version) {
            case 10:
                packet = HandshakeV10Packet.readHandshake(byteBuf);
                break;
            case 9:
            default:
                throw new JdbdRuntimeException(String.format("unsupported Handshake packet version[%s].", version)) {

                };
        }
        return packet;
    }

    private Mono<MySQLPacket> handleHandshakePacket(MySQLPacket packet) {
        Mono<MySQLPacket> mono;
        if (packet instanceof ErrorPacket) {
            //TODO zoro reject Protocol
            mono = Mono.error(new RuntimeException("handshake error."));
        } else if (packet instanceof HandshakeV10Packet) {
            this.handshakePacket.compareAndSet(null, (HandshakeV10Packet) packet);
            this.clientCollationIndex.compareAndSet(null, mapClientCollationIndex());
            mono = Mono.just(packet);
        } else if (packet instanceof HandshakeV9Packet) {
            this.handshakePacket.compareAndSet(null, (HandshakeV9Packet) packet);
            this.clientCollationIndex.compareAndSet(null, mapClientCollationIndex());
            mono = Mono.just(packet);
        } else {
            // never here
            mono = Mono.error(new IllegalArgumentException("packet error"));
        }
        return mono;
    }

    private Mono<MySQLPacket> writeHandshakeResponse320() {
        return Mono.empty();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private Mono<MySQLPacket> writeHandshakeResponse41() {
        final Charset charset = this.clientCharset;
        final int clientFlag = getClientFlat();

        final ByteBuf packetBuffer = PacketUtils.createPacketBuffer(this.connection, 1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, getClientCollationIndex());
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        HostInfo hostInfo = getHostInfo();
        String user = hostInfo.getUser();
        PacketUtils.writeStringTerm(packetBuffer, user.getBytes(charset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {

        } else {

        }
        return Mono.empty();
    }

    private int getClientFlat() {
        HandshakeV10Packet handshakeV10Packet = (HandshakeV10Packet) this.handshakePacket.get();
        final int serverFlag = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = StringUtils.hasText(this.mySQLUrl.getOriginalDatabase())
                && env.getProperty(PropertyKey.createDatabaseIfNotExist.getKeyName(), Boolean.class, Boolean.FALSE);

        return CLIENT_SECURE_CONNECTION
                | CLIENT_PLUGIN_AUTH
                | (serverFlag & CLIENT_LONG_PASSWORD)  //
                | (serverFlag & CLIENT_PROTOCOL_41)    //

                | (serverFlag & CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverFlag & CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverFlag & CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverFlag & CLIENT_LONG_FLAG)      //

                | (serverFlag & CLIENT_DEPRECATE_EOF)  //
                | (serverFlag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getRequiredProperty(PropertyKey.useCompression, Boolean.class) ? (serverFlag & CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverFlag & CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getRequiredProperty(PropertyKey.useAffectedRows, Boolean.class) ? 0 : (serverFlag & CLIENT_FOUND_ROWS))

                | (env.getRequiredProperty(PropertyKey.allowLoadLocalInfile, Boolean.class) ? (serverFlag & CLIENT_LOCAL_FILES) : 0)
                | (env.getRequiredProperty(PropertyKey.interactiveClient, Boolean.class) ? (serverFlag & CLIENT_INTERACTIVE) : 0)
                | (env.getRequiredProperty(PropertyKey.allowMultiQueries, Boolean.class) ? (serverFlag & CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getRequiredProperty(PropertyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverFlag & CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (NONE.equals(env.getRequiredProperty(PropertyKey.connectionAttributes)) ? 0 : (serverFlag & CLIENT_CONNECT_ATTRS))
                | (env.getRequiredProperty(PropertyKey.sslMode, SslMode.class) != SslMode.DISABLED ? (serverFlag & CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }


    private byte getClientCollationIndex() {
        Byte b = this.clientCollationIndex.get();
        if (b == null) {
            throw new IllegalStateException("client no handshake");
        }
        return b;
    }

    private byte mapClientCollationIndex() {
        int charsetIndex;
        AbstractHandshakePacket handshakePacket = this.handshakePacket.get();
        if (handshakePacket == null) {
            throw new IllegalStateException("client no handshake.");
        }
        charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(
                this.clientCharset.name(), handshakePacket.getServerVersion());
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        if (charsetIndex > 255) {
            throw new JdbdRuntimeException("client collation mapping error.") {
            };
        }
        return (byte) charsetIndex;
    }

    private HostInfo getHostInfo() {
        return this.mySQLUrl.getHosts().get(0);
    }

}
