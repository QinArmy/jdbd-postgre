package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.*;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.qinarmy.util.Pair;
import org.qinarmy.util.StringUtils;
import org.qinarmy.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.netty.NettyPipeline;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class ClientConnectionProtocolImpl implements ClientConnectionProtocol, ProtocolAssistant {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolImpl.class);


    public static Mono<ClientConnectionProtocol> from(MySQLUrl mySQLUrl) {
        if (mySQLUrl.getProtocol() != MySQLUrl.Protocol.SINGLE_CONNECTION) {
            throw new IllegalArgumentException(
                    String.format("mySQLUrl protocol isn't %s", MySQLUrl.Protocol.SINGLE_CONNECTION));
        }
        HostInfo hostInfo = mySQLUrl.getHosts().get(0);
        return TcpClient.create()
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .connect()
                // create ClientCommandProtocolImpl instance
                .map(connection -> new ClientConnectionProtocolImpl(mySQLUrl, connection))
                ;
    }


    private static final String NONE = "none";


    /**
     * below {@code xxx_PHASE } representing connection phase.
     *
     * @see #connectionPhase
     */
    private static final int HANDSHAKE_PHASE = 0;
    private static final int SSL_PHASE = 1;
    private static final int AUTHENTICATION_PHASE = 2;
    private static final int CONFIGURE_SESSION_PHASE = 3;

    private static final int INITIALIZING_PHASE = 4;

    private final MySQLUrl mySQLUrl;

    private final Connection connection;

    private final MySQLCumulateReceiver cumulateReceiver;

    private final AtomicReference<HandshakeV10Packet> handshakeV10Packet = new AtomicReference<>(null);

    private final Properties properties;

    /**
     * connection phase client charset,will send to server by {@code Protocol::HandshakeResponse41}.
     */
    private final Charset handshakeCharset;

    private final AtomicReference<Integer> negotiatedCapability = new AtomicReference<>(null);

    private final AtomicInteger sequenceId = new AtomicInteger(-1);

    private final AtomicReference<ServerStatus> oldServerStatus = new AtomicReference<>(null);

    private final AtomicReference<ServerStatus> serverStatus = new AtomicReference<>(null);

    private final AtomicInteger authCounter = new AtomicInteger(0);

    private final AtomicInteger connectionPhase = new AtomicInteger(HANDSHAKE_PHASE);

    private final AtomicInteger handshakeCollationIndex = new AtomicInteger(0);


    private ClientConnectionProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.properties = mySQLUrl.getHosts().get(0).getProperties();
        this.handshakeCharset = this.properties.getProperty(PropertyKey.characterEncoding
                , Charset.class, StandardCharsets.UTF_8);

        this.cumulateReceiver = MySQLCumulateSubscriber.from(connection);
    }


    @Override
    public Mono<Void> authenticateAndInitializing() {
        return receiveHandshake()
                .then(Mono.defer(this::sslNegotiate))
                .then(Mono.defer(this::authenticate))
                .then(Mono.defer(this::configureSessionProperties))
                .then(Mono.defer(this::initialize))
                ;
    }

    /*################################## blow ProtocolAssistant method ##################################*/

    @Override
    public Charset getHandshakeCharset() {
        return this.handshakeCharset;
    }

    @Override
    public Charset getPasswordCharset() {
        String pwdCharset = this.properties.getProperty(PropertyKey.passwordCharacterEncoding);
        return pwdCharset == null ? this.handshakeCharset : Charset.forName(pwdCharset);
    }

    @Override
    public HostInfo getMainHostInfo() {
        return this.mySQLUrl.getHosts().get(0);
    }

    @Override
    public boolean isUseSsl() {
        return (obtainNegotiatedCapability() & CLIENT_SSL) != 0;
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return PacketUtils.createPacketBuffer(this.connection, initialPayloadCapacity);
    }

    @Override
    public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
        return this.connection.outbound().alloc().buffer(initialPayloadCapacity);
    }

    @Override
    public ByteBuf createOneSizePayload(int payloadByte) {
        return this.connection.outbound().alloc().buffer(1)
                .writeByte(payloadByte)
                .asReadOnly();
    }

    @Override
    public ByteBuf createEmptyPayload() {
        return this.connection.outbound().alloc().buffer(0).asReadOnly();
    }

    @Override
    public ServerVersion getServerVersion() {
        return obtainHandshakeV10Packet().getServerVersion();
    }


    /*################################## blow package method ##################################*/


    /**
     * <p>
     * must invoke firstly .
     * </p>
     * <p>
     * Receive HandshakeV10 packet send by MySQL server.
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
     */
    Mono<MySQLPacket> receiveHandshake() {
        final int phase = this.connectionPhase.get();
        if (phase != HANDSHAKE_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return receivePayload()
                .flatMap(this::readHandshakeV10Packet)
                .flatMap(this::handleHandshakeV10Packet)
                .cast(MySQLPacket.class)
                ;
    }

    /**
     * negotiate ssl for connection than hold by this instance.
     * <p>
     * must invoke after {@link #receiveHandshake()} and before {@link #authenticateAndInitializing()}
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake_ssl_handshake">Protocol::SSL Handshake</a>
     */
    Mono<PropertyDefinitions.SslMode> sslNegotiate() {
        final int phase = this.connectionPhase.get();
        if (phase != SSL_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        if ((obtainNegotiatedCapability() & CLIENT_SSL) == 0) {
            return doAfterSendSslRequestEnd();
        }
        return sendHandshakePacket(createSslRequestPacket())
                .then(Mono.defer(this::performSslHandshake))
                .then(Mono.defer(this::doAfterSendSslRequestEnd))
                ;
    }

    /**
     * <p>
     * must invoke after {@link #sslNegotiate()}.
     * </p>
     * <p>
     * do below:
     *     <ol>
     *     <li>send HandshakeResponse41 packet.</li>
     *     <li>handle more authentication exchange.</li>
     *     </ol>
     * </p>
     */
    Mono<Void> authenticate() {
        final int phase = this.connectionPhase.get();
        if (phase != AUTHENTICATION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        final Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> triple;
        //1. obtain plugin
        triple = obtainAuthenticationPlugin();

        final AuthenticationPlugin plugin = triple.getFirst();
        //2. 'plugin out' is password data.
        ByteBuf pluginOut = createAuthenticationDataFor41(plugin, triple.getSecond());

        return createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut)
                // send response handshake packet
                .flatMap(this::sendHandshakePacket)
                .then(Mono.defer(this::receivePayload))
                // handle authentication Negotiation
                .flatMap(packet -> handleAuthResponse(packet, plugin, triple.getThird()))
                .then(Mono.defer(this::enableMultiStatement))
                ;
    }

    /**
     * <p>
     * must invoke after {@link #authenticateAndInitializing()}
     * </p>
     * configure below url config session group properties:
     * <ul>
     *     <li>{@link PropertyKey#sessionVariables}</li>
     *     <li>{@link PropertyKey#characterEncoding}</li>
     *     <li>{@link PropertyKey#characterSetResults}</li>
     *     <li>{@link PropertyKey#connectionCollation}</li>
     * </ul>
     * <p>
     *     If {@link PropertyKey#detectCustomCollations} is true,then firstly handle this.
     * </p>
     */
    Mono<Void> configureSessionProperties() {
        final int phase = this.connectionPhase.get();
        if (phase != CONFIGURE_SESSION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return detectCustomCollations()
                .then(Mono.defer(this::executeSessionVariables))
                .then(Mono.defer(this::configureSessionCharset))
                .then(Mono.defer(this::doAfterConfigureSessionProperties))
                ;
    }

    /**
     * <p>
     * must invoke after {@link #configureSessionProperties()}
     * </p>
     * <p>
     * do initialize ,must contain below operation:
     *     <ul>
     *         <li>{@code set autocommit = 0}</li>
     *         <li>{@code SET SESSION TRANSACTION READ COMMITTED}</li>
     *         <li>more initializing operations</li>
     *     </ul>
     * </p>
     */
    Mono<Void> initialize() {
        final int phase = this.connectionPhase.get();
        if (phase != INITIALIZING_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return Mono.empty()
                .then(Mono.defer(this::disableMultiStatement))
                ;
    }


    MySQLUrl getMySQLUrl() {
        return this.mySQLUrl;
    }

    Connection getConnection() {
        return this.connection;
    }

    MySQLCumulateReceiver getCumulateReceiver() {
        return this.cumulateReceiver;
    }

    HandshakeV10Packet getHandshakeV10Packet() {
        return obtainHandshakeV10Packet();
    }

    byte getClientCollationIndex() {
        return obtainHandshakeCollationIndex();
    }

    int getNegotiatedCapability() {
        return obtainNegotiatedCapability();
    }



    /*################################## blow private method ##################################*/

    private Mono<Void> handleAuthResponse(ByteBuf payloadBuf, AuthenticationPlugin plugin
            , Map<String, AuthenticationPlugin> pluginMap) {

        Mono<Void> mono;
        if (this.authCounter.addAndGet(1) >= 100) {
            mono = Mono.error(new JdbdMySQLException("TooManyAuthenticationPluginNegotiations"));
        } else if (OkPacket.isOkPacket(payloadBuf)) {
            OkPacket packet = OkPacket.readPacket(payloadBuf, obtainNegotiatedCapability());
            setServerStatus(packet.getStatusFags(), true);
            LOG.debug("MySQL authentication success,info:{}", packet.getInfo());
            mono = Mono.empty();
        } else if (ErrorPacket.isErrorPacket(payloadBuf)) {
            ErrorPacket packet;
            if (this.sequenceId.get() < 2) {
                packet = ErrorPacket.readPacketInHandshakePhase(payloadBuf);
            } else {
                packet = ErrorPacket.readPacket(payloadBuf, obtainNegotiatedCapability());
            }
            mono = rejectPacket(new JdbdMySQLException("auth error, %s", packet));
        } else {
            mono = processNextAuthenticationNegotiation(payloadBuf, plugin, pluginMap);
        }
        return mono;
    }

    private Mono<Void> processNextAuthenticationNegotiation(ByteBuf payloadBuf, AuthenticationPlugin plugin
            , Map<String, AuthenticationPlugin> pluginMap) {
        final AuthenticationPlugin authPlugin;
        if (PacketUtils.isAuthSwitchRequestPacket(payloadBuf)) {
            payloadBuf.skipBytes(1); // skip type header
            String pluginName = PacketUtils.readStringTerm(payloadBuf, StandardCharsets.US_ASCII);

            if (plugin.getProtocolPluginName().equals(pluginName)) {
                authPlugin = plugin;
            } else {
                authPlugin = pluginMap.get(pluginName);
                if (authPlugin == null) {
                    return Mono.error(new JdbdMySQLException("BadAuthenticationPlugin[%s] from server.", pluginName));
                }
            }
            authPlugin.reset();
        } else {
            authPlugin = plugin;
            payloadBuf.skipBytes(1); // skip type header
        }
        // plugin auth
        return Flux.fromIterable(authPlugin.nextAuthenticationStep(payloadBuf))
                .map(this::convertPayloadBufToPacketBuf)
                // all packet send to server
                .flatMap(this::sendHandshakePacket)
                .then(Mono.defer(this::receivePayload))
                // recursion invoke handleAuthResponse
                .flatMap(buffer -> handleAuthResponse(buffer, authPlugin, pluginMap));
    }

    private ByteBuf convertPayloadBufToPacketBuf(ByteBuf payloadBuf) {
        ByteBuf packetBuf = createPacketBuffer(payloadBuf.readableBytes());
        payloadBuf.readBytes(packetBuf);
        payloadBuf.release();
        return packetBuf;
    }

    private void setServerStatus(int serverStatus, boolean saveOldStatusFlags) {
        if (saveOldStatusFlags) {
            this.oldServerStatus.set(this.serverStatus.get());
        } else {
            this.oldServerStatus.set(null);
        }
        this.serverStatus.set(ServerStatus.fromValue(serverStatus));
    }

    private Mono<HandshakeV10Packet> handleHandshakeV10Packet(HandshakeV10Packet packet) {
        if (!this.handshakeV10Packet.compareAndSet(null, packet)) {
            return Mono.error(new JdbdMySQLException(
                    "%s can't concurrently invoke.handshakeV10Packet isn't null.", this));
        }
        final byte handshakeCollationIndex;
        handshakeCollationIndex = obtainHandshakeCollationIndex(this.handshakeCharset, packet.getServerVersion());
        if (!this.handshakeCollationIndex.compareAndSet(0, handshakeCollationIndex)) {
            return Mono.error(new JdbdMySQLException(
                    "%s can't concurrently invoke.handshakeCollationIndex expected[0] but not.", this));
        }
        return this.connectionPhase.compareAndSet(HANDSHAKE_PHASE, SSL_PHASE)
                ? Mono.just(packet)
                : createConcurrentlyConnectionException(HANDSHAKE_PHASE);
    }


    private Mono<HandshakeV10Packet> readHandshakeV10Packet(ByteBuf payloadBuf) {
        if (ErrorPacket.isErrorPacket(payloadBuf)) {
            // reject packet
            ErrorPacket errorPacket = ErrorPacket.readPacket(payloadBuf, 0);
            return FutureMono.from(this.connection.channel().close())
                    .then(Mono.defer(() ->
                            Mono.error(new JdbdMySQLException(
                                    "handshake packet is error packet,close connection.ErrorPacket[%s]", errorPacket))
                    ));
        }
        return Mono.just(HandshakeV10Packet.readHandshake(payloadBuf));
    }

    /**
     * @return read-only {@link ByteBuf}
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private Mono<ByteBuf> createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = obtainNegotiatedCapability();

        final ByteBuf packetBuffer = createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, obtainHandshakeCollationIndex());
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        HostInfo hostInfo = getMainHostInfo();
        String user = hostInfo.getUser();
        PacketUtils.writeStringTerm(packetBuffer, user.getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            PacketUtils.writeStringLenEnc(packetBuffer, pluginOut);
        } else {
            packetBuffer.writeByte(pluginOut.readableBytes());
            packetBuffer.writeBytes(pluginOut);
        }
        pluginOut.release();

        // 7. database
        if ((clientFlag & CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.mySQLUrl.getOriginalDatabase();
            if (!MySQLStringUtils.hasText(database)) {
                throw new JdbdMySQLException("client flag error,check this.getClientFlat() method.");
            }
            PacketUtils.writeStringTerm(packetBuffer, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & CLIENT_PLUGIN_AUTH) != 0) {
            PacketUtils.writeStringTerm(packetBuffer, authPluginName.getBytes(clientCharset));
        }
        // 9. client connection attributes
        if ((clientFlag & CLIENT_CONNECT_ATTRS) != 0) {
            Map<String, String> propertySource = createConnectionAttributes();
            // length of all key-values,affected rows
            PacketUtils.writeIntLenEnc(packetBuffer, propertySource.size());
            for (Map.Entry<String, String> e : propertySource.entrySet()) {
                // write key
                PacketUtils.writeStringLenEnc(packetBuffer, e.getKey().getBytes(clientCharset));
                // write value
                PacketUtils.writeStringLenEnc(packetBuffer, e.getValue().getBytes(clientCharset));
            }

        }
        //TODO 10.zstd_compression_level,compression level for zstd compression algorithm
        //packetBuffer.writeByte(0);
        return Mono.just(packetBuffer);
    }

    /**
     * create ssl request packet.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake_ssl_handshake">Protocol::SSL Handshake</a>
     */
    private ByteBuf createSslRequestPacket() {
        ByteBuf packetBuf = createPacketBuffer(32);

        final int serverCapability = obtainHandshakeV10Packet().getCapabilityFlags();
        if ((serverCapability & CLIENT_PROTOCOL_41) != 0) {
            // 1. client_flag
            PacketUtils.writeInt4(packetBuf, obtainNegotiatedCapability());
            // 2. max_packet_size
            PacketUtils.writeInt4(packetBuf, MAX_PACKET_SIZE);
            // 3. character_set,
            PacketUtils.writeInt1(packetBuf, obtainHandshakeCollationIndex());
            // 4. filler
            packetBuf.writeZero(23);
        } else {
            // 1. client_flag
            PacketUtils.writeInt2(packetBuf, obtainNegotiatedCapability());
            // 2. max_packet_size
            PacketUtils.writeInt3(packetBuf, MAX_PACKET_SIZE);
        }
        return packetBuf;
    }

    private Mono<Void> performSslHandshake() {

        Channel channel = this.connection.channel();
        final SslHandler sslHandler = createSslHandler(channel);

        ChannelPipeline pipeline = channel.pipeline();

        if (pipeline.get(NettyPipeline.ProxyHandler) != null) {
            pipeline.addAfter(NettyPipeline.ProxyHandler, NettyPipeline.SslHandler, sslHandler);
        } else if (pipeline.get(NettyPipeline.ProxyProtocolReader) != null) {
            pipeline.addAfter(NettyPipeline.ProxyProtocolReader, NettyPipeline.SslHandler, sslHandler);
        } else {
            pipeline.addFirst(NettyPipeline.SslHandler, sslHandler);
        }
        return Mono.empty();
    }

    private SslHandler createSslHandler(Channel channel) {
        TrustManagerFactory trustManagerFactory = tryObtainTrustManagerFactory();
        KeyManagerFactory keyManagerFactory = tryObtainKeyManagerFactory();

        SslContextBuilder contextBuilder = SslContextBuilder.forClient();
        if (trustManagerFactory != null) {
            contextBuilder.trustManager(trustManagerFactory);
        }
        if (keyManagerFactory != null) {
            contextBuilder.keyManager(keyManagerFactory);
        }
        contextBuilder.protocols(obtainAllowedTlsProtocolList())
        //TODO zoro 找到确认 jdk or open ssl 支持 cipher suit 的方法
        //.ciphers(obtainAllowedCipherSuitList())
        ;
        try {
            HostInfo hostInfo = getMainHostInfo();
            return contextBuilder
                    .build()
                    .newHandler(channel.alloc(), hostInfo.getHost(), hostInfo.getPort());
        } catch (SSLException e) {
            throw new JdbdMySQLException(e, "create %s", SslContext.class.getName());
        }
    }


    private List<String> obtainAllowedCipherSuitList() {
        String enabledSSLCipherSuites = this.properties.getProperty(PropertyKey.enabledSSLCipherSuites);
        List<String> candidateList = StringUtils.spitAsList(enabledSSLCipherSuites, ",");

        if (candidateList.isEmpty()) {
            candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST;
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST);
        }

        List<String> allowedCipherList = new ArrayList<>(candidateList.size());
        candidateFor:
        for (String candidate : candidateList) {
            for (String restricted : ProtocolUtils.MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST) {
                if (candidate.contains(restricted)) {
                    continue candidateFor;
                }
            }
            allowedCipherList.add(candidate);
        }
        return Collections.unmodifiableList(allowedCipherList);
    }


    /**
     * @return a unmodifiable list
     */
    private List<String> obtainAllowedTlsProtocolList() {
        String enabledTLSProtocols = this.properties.getProperty(PropertyKey.enabledTLSProtocols);
        List<String> candidateList = StringUtils.spitAsList(enabledTLSProtocols, ",");

        if (candidateList.isEmpty()) {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();

            if (serverVersion.meetsMinimum(5, 7, 28)
                    || (serverVersion.meetsMinimum(5, 6, 46) && !serverVersion.meetsMinimum(5, 7, 0))
                    || (serverVersion.meetsMinimum(5, 6, 0) && ServerVersion.isEnterpriseEdition(serverVersion))) {
                candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST;
            } else {
                candidateList = Collections.unmodifiableList(Arrays.asList(ProtocolUtils.TLSv1_1, ProtocolUtils.TLSv1));
            }
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST);
            candidateList = Collections.unmodifiableList(candidateList);
        }
        return candidateList;
    }

    @Nullable
    private TrustManagerFactory tryObtainTrustManagerFactory() {
        PropertyDefinitions.SslMode sslMode;
        sslMode = this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class);
        if (sslMode != PropertyDefinitions.SslMode.VERIFY_CA
                && sslMode != PropertyDefinitions.SslMode.VERIFY_IDENTITY) {
            return null;
        }
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(false);
            if (storePair == null) {
                return null;
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(storePair.getFirst());
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", TrustManagerFactory.class.getName());
        } catch (KeyStoreException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.trustCertificateKeyStoreUrl, TrustManagerFactory.class.getName());
        }
    }

    @Nullable
    private KeyManagerFactory tryObtainKeyManagerFactory() {
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(true);
            if (storePair == null) {
                return null;
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(storePair.getFirst(), storePair.getSecond());
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", KeyManagerFactory.class.getName());
        } catch (KeyStoreException | UnrecoverableKeyException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.clientCertificateKeyStoreUrl, KeyManagerFactory.class.getName());
        }
    }

    @Nullable
    private Pair<KeyStore, char[]> tryObtainKeyStorePasswordPairForSsl(final boolean client) {
        // 1. below obtain three storeUrl,storeType,storePassword
        final PropertyKey storeUrlKey, storeTypeKey, passwordKey;
        final String defaultStoreUrlKey, defaultStoreTypeKey, defaultPasswordKey;
        if (client) {
            storeUrlKey = PropertyKey.clientCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.clientCertificateKeyStoreType;
            passwordKey = PropertyKey.clientCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.keyStore";
            defaultStoreTypeKey = "javax.net.ssl.keyStoreType";
            defaultPasswordKey = "javax.net.ssl.keyStorePassword";
        } else {
            storeUrlKey = PropertyKey.trustCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.trustCertificateKeyStoreType;
            passwordKey = PropertyKey.trustCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.trustStore";
            defaultStoreTypeKey = "javax.net.ssl.trustStoreType";
            defaultPasswordKey = "javax.net.ssl.trustStorePassword";
        }

        final Properties properties = this.properties;
        String storeUrl, storeType;
        storeUrl = properties.getProperty(storeUrlKey);
        storeType = properties.getProperty(storeTypeKey);

        if (!StringUtils.hasText(storeUrl)) {
            storeUrl = System.getProperty(defaultStoreUrlKey);
        }
        if (!StringUtils.hasText(storeType)) {
            storeType = System.getProperty(defaultStoreTypeKey);
        }

        if (!StringUtils.hasText(storeUrl) || !StringUtils.hasText(storeType)) {
            return null;
        }
        String storePwd = properties.getProperty(passwordKey);
        if (!StringUtils.hasText(storePwd)) {
            storePwd = System.getenv(defaultPasswordKey);
        }

        final char[] storePassword = (storePwd == null) ? new char[0] : storePwd.toCharArray();
        // 2. create and init KeyStore with three storeUrl,storeType,storePassword
        try (InputStream storeInput = new URL(storeUrl).openStream()) {
            try {
                KeyStore keyStore = KeyStore.getInstance(storeType);
                keyStore.load(storeInput, storePassword);
                return new Pair<>(keyStore, storePassword);
            } catch (KeyStoreException e) {
                throw new JdbdMySQLException(e, "%s[%s] error,cannot create %s.", storeTypeKey, storeType
                        , KeyStore.class.getName());
            } catch (NoSuchAlgorithmException | IOException | CertificateException e) {
                throw new JdbdMySQLException(e, "%s[%s] content error,cannot init %s."
                        , storeUrlKey, storeUrl, KeyStore.class.getName());
            }
        } catch (MalformedURLException e) {
            throw new JdbdMySQLException(e, "%s [%s] isn't url.", storeUrlKey, storeUrl);
        } catch (IOException e) {
            throw new JdbdMySQLException(e, "%s [%s] cannot open InputStream.", storeUrlKey, storeUrl);
        }
    }


    private Mono<Void> enableMultiStatement() {
        return Mono.empty();
    }

    private Mono<Void> disableMultiStatement() {
        return Mono.empty();
    }

    private Mono<PropertyDefinitions.SslMode> doAfterSendSslRequestEnd() {
        return this.connectionPhase.compareAndSet(SSL_PHASE, AUTHENTICATION_PHASE)
                ? Mono.justOrEmpty(this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class))
                : createConcurrentlyConnectionException(SSL_PHASE);
    }

    private Mono<Void> detectCustomCollations() {
        if (!this.properties.getRequiredProperty(PropertyKey.detectCustomCollations, Boolean.class)) {
            return Mono.empty();
        }
        return detectCustomCollation()
                .then(Mono.defer(this::detectCustomCharset))
                ;
    }


    private Mono<Void> executeSessionVariables() {
        return Mono.empty();
    }

    private Mono<Void> configureSessionCharset() {
        return Mono.empty();
    }

    private Mono<Void> doAfterConfigureSessionProperties() {
        return this.connectionPhase.compareAndSet(CONFIGURE_SESSION_PHASE, INITIALIZING_PHASE)
                ? Mono.empty()
                : createConcurrentlyConnectionException(CONFIGURE_SESSION_PHASE);
    }

    private <T> Mono<T> createConcurrentlyConnectionException(int expected) {
        return Mono.error(new JdbdMySQLException(
                "%s can't concurrently invoke.connectionPhase expected[%s] but not.", this, expected));
    }

    private <T> Mono<T> createConnectionPhaseNotMatchException(int currentPhase) {
        return Mono.error(new JdbdMySQLException("Not sslNegotiate phase,current phase[%s]", currentPhase));
    }

    private Mono<MySQLRowMeta> executeQueryCommand(String command, final AtomicInteger sequenceId) {
        // 1. create COM_QUERY packet.
        ByteBuf packetBuf = createPacketBuffer(command.length());
        packetBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
                // here still use  handshakeCharset TODO think right?
                .writeBytes(command.getBytes(this.handshakeCharset));
        return sendPacket(packetBuf, sequenceId) //2. send COM_QUERY packet
                .then(Mono.defer(this::receiveResultSetMetadataPacket))//3. receive row meta packet
                .map(metaBuf -> handleResultRowMetadataPacket(metaBuf, sequenceId)) //3. convert row meta packet to MySQLRowMeta instance
                ;
    }

    private Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = loadAuthenticationPluginMap();

        Properties properties = this.properties;
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        String pluginName = handshakeV10Packet.getAuthPluginName();

        AuthenticationPlugin plugin = pluginMap.get(pluginName);
        boolean skipPassword = false;
        final boolean useSsl = isUseSsl();
        if (plugin == null) {
            plugin = pluginMap.get(properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin));
        } else if (Sha256PasswordPlugin.PLUGIN_NAME.equals(pluginName)
                && !useSsl
                && properties.getProperty(PropertyKey.serverRSAPublicKeyFile) == null
                && !properties.getRequiredProperty(PropertyKey.allowPublicKeyRetrieval, Boolean.class)) {
            /*
             * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
             * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
             * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
             * sent to avoid subjecting it to lesser security levels.
             */
            String defaultPluginName = properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin);
            skipPassword = !pluginName.equals(defaultPluginName);
            plugin = pluginMap.get(defaultPluginName);
        }
        if (plugin.requiresConfidentiality() && !useSsl) {
            throw new JdbdMySQLException("AuthenticationPlugin[%s] required SSL", plugin.getClass().getName());
        }
        return new Triple<>(plugin, skipPassword, pluginMap);
    }

    private int obtainNegotiatedCapability() {
        Integer clientCapability = this.negotiatedCapability.get();
        if (clientCapability == null) {
            clientCapability = createNegotiatedCapability();
            if (!this.negotiatedCapability.compareAndSet(null, clientCapability)) {
                clientCapability = this.negotiatedCapability.get();
            }
        }
        return clientCapability;
    }

    private int createNegotiatedCapability() {
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        final int serverCapability = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = MySQLStringUtils.hasText(this.mySQLUrl.getOriginalDatabase())
                && !env.getRequiredProperty(PropertyKey.createDatabaseIfNotExist, Boolean.class);

        return CLIENT_SECURE_CONNECTION
                | CLIENT_PLUGIN_AUTH
                | (serverCapability & CLIENT_LONG_PASSWORD)  //
                | (serverCapability & CLIENT_PROTOCOL_41)    //

                | (serverCapability & CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & CLIENT_LONG_FLAG)      //

                | (serverCapability & CLIENT_DEPRECATE_EOF)  //
                | (serverCapability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getRequiredProperty(PropertyKey.useCompression, Boolean.class) ? (serverCapability & CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getRequiredProperty(PropertyKey.useAffectedRows, Boolean.class) ? 0 : (serverCapability & CLIENT_FOUND_ROWS))

                | (env.getRequiredProperty(PropertyKey.allowLoadLocalInfile, Boolean.class) ? (serverCapability & CLIENT_LOCAL_FILES) : 0)
                | (env.getRequiredProperty(PropertyKey.interactiveClient, Boolean.class) ? (serverCapability & CLIENT_INTERACTIVE) : 0)
                | (env.getRequiredProperty(PropertyKey.allowMultiQueries, Boolean.class) ? (serverCapability & CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getRequiredProperty(PropertyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverCapability & CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (NONE.equals(env.getProperty(PropertyKey.connectionAttributes)) ? 0 : (serverCapability & CLIENT_CONNECT_ATTRS))
                | (env.getRequiredProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class) != PropertyDefinitions.SslMode.DISABLED ? (serverCapability & CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }

    private byte obtainHandshakeCollationIndex() {
        int index = this.handshakeCollationIndex.get();
        if (index == 0) {
            throw new IllegalStateException("client no handshake,handshakeCollationIndex no value.");
        }
        return (byte) index;
    }

    /**
     * @return a unmodifiable map
     */
    private Map<String, AuthenticationPlugin> loadAuthenticationPluginMap() {
        Properties properties = this.properties;

        String defaultPluginName = properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin);
        List<String> disabledPluginList = properties.getPropertyList(PropertyKey.disabledAuthenticationPlugins);

        // below three line: obtain pluginClassNameList
        List<String> pluginClassNameList = properties.getPropertyList(PropertyKey.authenticationPlugins);
        pluginClassNameList.add(defaultPluginName);
        appendBuildInPluginClassNameList(pluginClassNameList);

        // below create AuthenticationPlugin map
        HostInfo hostInfo = getMainHostInfo();
        Map<String, AuthenticationPlugin> map = new HashMap<>();

        boolean defaultIsFound = false;
        for (String pluginClassName : pluginClassNameList) {
            if (disabledPluginList.contains(pluginClassName)) {
                continue;
            }
            AuthenticationPlugin plugin = loadPlugin(pluginClassName, this, hostInfo);
            String pluginName = plugin.getProtocolPluginName();
            if (disabledPluginList.contains(pluginName)) {
                continue;
            }
            map.put(pluginName, plugin);
            if (pluginClassName.equals(defaultPluginName)) {
                defaultIsFound = true;
            }
        }
        if (!defaultIsFound) {
            throw new JdbdMySQLException("defaultAuthenticationPlugin[%s] not fond or disable.", defaultPluginName);
        }
        return Collections.unmodifiableMap(map);
    }


    private byte mapClientCollationIndex() {
        int charsetIndex;
        HandshakeV10Packet handshakePacket = this.handshakeV10Packet.get();
        if (handshakePacket == null) {
            throw new IllegalStateException("client no handshake.");
        }
        charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(
                this.handshakeCharset.name(), handshakePacket.getServerVersion());
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        if (charsetIndex > 255) {
            throw new JdbdRuntimeException("client collation mapping error.") {
            };
        }
        return (byte) charsetIndex;
    }


    private HandshakeV10Packet obtainHandshakeV10Packet() {
        HandshakeV10Packet packet = this.handshakeV10Packet.get();
        if (packet == null) {
            throw new IllegalStateException("no handshake.");
        }
        return packet;
    }


    private ByteBuf createAuthenticationDataFor41(AuthenticationPlugin plugin, boolean skipPassword) {
        ByteBuf payloadBuf;
        if (skipPassword) {
            // skip password
            payloadBuf = createOneSizePayload(0);
        } else {
            HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
            String seed = handshakeV10Packet.getAuthPluginDataPart1() + handshakeV10Packet.getAuthPluginDataPart2();
            byte[] seedBytes = seed.getBytes();
            ByteBuf fromServer = createPayloadBuffer(seedBytes.length);
            fromServer.writeBytes(seedBytes);
            // invoke AuthenticationPlugin
            List<ByteBuf> toServer = plugin.nextAuthenticationStep(fromServer);
            // release temp fromServer
            fromServer.release();
            if (toServer.isEmpty()) {
                throw new IllegalStateException(String.format(
                        "AuthenticationPlugin[%s] nextAuthenticationStep return error"
                        , plugin.getClass().getName()));
            }
            payloadBuf = toServer.get(0);
        }
        return payloadBuf;
    }


    private Map<String, String> createConnectionAttributes() {
        String connectionStr = this.properties.getProperty(PropertyKey.connectionAttributes);
        Map<String, String> attMap = new HashMap<>();

        if (connectionStr != null) {
            String[] pairArray = connectionStr.split(",");
            for (String pair : pairArray) {
                String[] kv = pair.split(":");
                if (kv.length != 2) {
                    throw new IllegalStateException(String.format("key[%s] can't resolve pair." +
                            "", PropertyKey.connectionAttributes));
                }
                attMap.put(kv[0].trim(), kv[1].trim());
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        attMap.put("_client_name", Constants.CJ_NAME);
        attMap.put("_client_version", Constants.CJ_VERSION);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_client_license", Constants.CJ_LICENSE);
        return attMap;
    }


    private Mono<Void> detectCustomCollation() {
        final AtomicInteger sequenceId = new AtomicInteger(-1);
        return executeQueryCommand("SHOW COLLATION", sequenceId)
                .flatMap(this::extractCollationRowData);
    }

    private Mono<Void> detectCustomCharset() {
        final AtomicInteger sequenceId = new AtomicInteger(-1);
        return executeQueryCommand("SHOW CHARSET", sequenceId)
                .flatMap(this::extractCharsetRowData);
    }


    /**
     * packet header have read.
     *
     * @see #readPacketHeader(ByteBuf)
     * @see #sendHandshakePacket(ByteBuf)
     */
    private Mono<ByteBuf> receivePayload() {
        return this.cumulateReceiver.receiveOnePacket()
                .flatMap(this::readPacketHeader);
    }

    private Mono<ByteBuf> receiveResultSetMetadataPacket() {
        return this.cumulateReceiver.receiveOne(this::comQueryResultSetMetaDecoder);
    }

    private Flux<ByteBuf> receiveMultiRowData() {
        return this.cumulateReceiver.receive(PacketDecoders::resultSetMultiRowDecoder);
    }

    private Mono<Void> extractCollationRowData(MySQLRowMeta metadata) {
        return receiveMultiRowData()
                .then()
                ;
    }

    private Mono<Void> extractCharsetRowData(MySQLRowMeta metadata) {
        return Mono.empty();
    }

    /**
     * @see #receivePayload()
     * @see #sendHandshakePacket(ByteBuf)
     */
    private Mono<ByteBuf> readPacketHeader(ByteBuf packetBuf) {
        packetBuf.skipBytes(3); // skip payload length
        int sequenceId = PacketUtils.readInt1(packetBuf);
        Mono<ByteBuf> mono;
        if (this.sequenceId.compareAndSet(sequenceId - 1, sequenceId)) {
            mono = Mono.just(packetBuf);
        } else {
            mono = Mono.error(new JdbdMySQLException(
                    "sequenceId[%s] form server error,should be %s .", sequenceId, sequenceId - 1));
        }
        return mono;

    }

    @Nullable
    private ByteBuf comQueryResultSetMetaDecoder(ByteBuf cumulateBuf) {
        return PacketDecoders.comQueryResponseDecoder(cumulateBuf, obtainNegotiatedCapability());
    }


    private MySQLRowMeta handleResultRowMetadataPacket(ByteBuf packetBuf, AtomicInteger sequenceId) {
        PacketDecoders.ComQueryResponse response;
        response = PacketDecoders.decodeComQueryResponseType(packetBuf, obtainNegotiatedCapability());
        if (response == PacketDecoders.ComQueryResponse.TEXT_RESULT) {
            packetBuf.skipBytes(3); // skip payload length
            sequenceId.set(PacketUtils.readInt1(packetBuf));
            // due to here character_set_results unknown, use UTF_8 ,see https://dev.mysql.com/doc/refman/8.0/en/charset-metadata.html
            // and https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_character_set_results
            return PacketDecoders.readResultRowMeta(packetBuf, obtainNegotiatedCapability()
                    , StandardCharsets.UTF_8, Collections.emptyMap());
        } else {
            throw new JdbdMySQLException("Expected result set response,but %s ", response);
        }
    }

    /**
     * @see #readPacketHeader(ByteBuf)
     * @see #receivePayload()
     */
    private Mono<Void> sendHandshakePacket(ByteBuf packetBuffer) {
        return sendPacket(packetBuffer, this.sequenceId);
    }

    private Mono<Void> sendPacket(ByteBuf packetBuffer, AtomicInteger sequenceId) {
        PacketUtils.writePacketHeader(packetBuffer, sequenceId.addAndGet(1));
        return Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuffer)));
    }

    private Mono<Void> closeConnection() {
        return FutureMono.from(this.connection.channel().close());
    }

    private <T> Mono<T> rejectPacket(Throwable rejectReason) {
        return closeConnection()
                .then(Mono.error(rejectReason));
    }


    /*################################## blow static method ##################################*/

    private static AuthenticationPlugin loadPlugin(String pluginClassName, ProtocolAssistant assistant
            , HostInfo hostInfo) {
        try {
            Class<?> pluginClass = Class.forName(convertPluginClassName(pluginClassName));
            if (!AuthenticationPlugin.class.isAssignableFrom(pluginClass)) {
                throw new JdbdMySQLException("class[%s] isn't %s type.", AuthenticationPlugin.class.getName());
            }
            Method method = pluginClass.getMethod("getInstance", ProtocolAssistant.class, HostInfo.class);
            if (!AuthenticationPlugin.class.isAssignableFrom(method.getReturnType())) {
                throw new JdbdMySQLException("plugin[%s] getInstance return error type.", pluginClassName);
            }
            return (AuthenticationPlugin) method.invoke(null, assistant, hostInfo);
        } catch (ClassNotFoundException e) {
            throw new JdbdMySQLException(e, "plugin[%s] not found in classpath.", pluginClassName);
        } catch (NoSuchMethodException e) {
            throw new JdbdMySQLException(e, "plugin[%s] no getInstance() factory method.", pluginClassName);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new JdbdMySQLException(e, "plugin[%s] getInstance() invoke error.", pluginClassName);
        }
    }


    private static void appendBuildInPluginClassNameList(List<String> pluginClassNameList) {
        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());
        pluginClassNameList.add(MySQLClearPasswordPlugin.class.getName());
        pluginClassNameList.add(Sha256PasswordPlugin.class.getName());
        pluginClassNameList.add(CachingSha2PasswordPlugin.class.getName());

        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());

    }

    private static String convertPluginClassName(String pluginClassName) {
        String className;
        switch (pluginClassName) {
            case MySQLNativePasswordPlugin.PLUGIN_NAME:
            case MySQLNativePasswordPlugin.PLUGIN_CLASS:
                className = MySQLNativePasswordPlugin.class.getName();
                break;
            case CachingSha2PasswordPlugin.PLUGIN_NAME:
            case CachingSha2PasswordPlugin.PLUGIN_CLASS:
                className = CachingSha2PasswordPlugin.class.getName();
                break;
            case MySQLClearPasswordPlugin.PLUGIN_NAME:
            case MySQLClearPasswordPlugin.PLUGIN_CLASS:
                className = MySQLClearPasswordPlugin.class.getName();
                break;
            case MySQLOldPasswordPlugin.PLUGIN_NAME:
            case MySQLOldPasswordPlugin.PLUGIN_CLASS:
                className = MySQLOldPasswordPlugin.class.getName();
                break;
            case Sha256PasswordPlugin.PLUGIN_NAME:
            case Sha256PasswordPlugin.PLUGIN_CLASS:
                className = Sha256PasswordPlugin.class.getName();
                break;
            default:
                className = pluginClassName;
        }
        return className;
    }


    private static byte obtainHandshakeCollationIndex(Charset handshakeCharset, ServerVersion serverVersion) {
        int charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(handshakeCharset.name(), serverVersion);
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        return (byte) charsetIndex;
    }


}
