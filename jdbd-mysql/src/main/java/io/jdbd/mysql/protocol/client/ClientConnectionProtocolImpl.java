package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.*;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.netty.tcp.TcpClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class ClientConnectionProtocolImpl implements ClientConnectionProtocol, ProtocolAssistant {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolImpl.class);


    public static Mono<ClientConnectionProtocol> getInstance(MySQLUrl mySQLUrl) {
        if (mySQLUrl.getProtocol() != MySQLUrl.Protocol.SINGLE_CONNECTION) {
            throw new IllegalArgumentException(
                    String.format("mySQLUrl protocol isn't %s", MySQLUrl.Protocol.SINGLE_CONNECTION));
        }
        HostInfo hostInfo = mySQLUrl.getHosts().get(0);
        return TcpClient.create()
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                // MySQLProtocolDecodeHandler splits mysql packet.
                .doOnConnected(MySQLProtocolDecodeHandler::addMySQLDecodeHandler)
                .connect()
                // create ClientCommandProtocolImpl instance
                .map(connection -> new ClientConnectionProtocolImpl(mySQLUrl, connection))
                ;
    }


    private static final String NONE = "none";

    private final MySQLUrl mySQLUrl;

    private final Connection connection;

    private final MySQLPacketSubscriber<ByteBuf> packetReceiver = new MySQLPacketSubscriber<>();

    private final AtomicReference<HandshakeV10Packet> handshakeV10Packet = new AtomicReference<>(null);

    private final Properties properties;

    private final Charset clientCharset;

    private final AtomicReference<Byte> clientCollationIndex = new AtomicReference<>(null);

    private final AtomicBoolean useSsl = new AtomicBoolean(true);

    private final AtomicInteger sequenceId = new AtomicInteger(0);

    private final AtomicReference<Integer> clientCapability = new AtomicReference<>(null);

    private final AtomicReference<ServerStatus> oldServerStatus = new AtomicReference<>(null);

    private final AtomicReference<ServerStatus> serverStatus = new AtomicReference<>(null);

    private final AtomicInteger authCounter = new AtomicInteger(0);


    private ClientConnectionProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.properties = mySQLUrl.getHosts().get(0).getProperties();
        this.clientCharset = Charset.forName(this.properties.getRequiredProperty(PropertyKey.characterEncoding));

        connection.inbound().receive().subscribe(packetReceiver);
    }

    @Override
    public Mono<MySQLPacket> ssl() {
        return Mono.empty();
//        return createSslRequestPayload()
//                .flatMap(this::sendPacket)
//                .then(Mono.empty())
//                ;
    }

    @Override
    public Mono<MySQLPacket> receiveHandshake() {
        return receivePayload()
                .flatMap(this::receiveHandshakeV10Packet)
                .doOnNext(this::handleHandshakeV10Packet)
                .cast(MySQLPacket.class)
                ;
    }

    @Override
    public Mono<MySQLPacket> responseHandshake() {
        final Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> triple;
        //1. obtain plugin
        triple = obtainAuthenticationPlugin();

        final AuthenticationPlugin plugin = triple.getFirst();
        //2. 'plugin out' is password data.
        ByteBuf pluginOut = createAuthenticationDataFor41(plugin, triple.getSecond());

        return createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut)
                .flatMap(this::sendPacket)
                .then(Mono.defer(this::receivePayload))
                .flatMap(packet -> handleAuthResponse(packet, plugin, triple.getThird()))
                .then(Mono.empty())
                ;
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
        return this.useSsl.get();
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


    private MySQLPacket doReadAuthResponse(ByteBuf payloadBuf) {
        MySQLPacket packet;
        if (OkPacket.isOkPacket(payloadBuf)) {
            packet = OkPacket.readPacket(payloadBuf, obtainHandshakeV10Packet().getCapabilityFlags());
        } else if (PacketUtils.isAuthSwitchRequestPacket(payloadBuf)) {
            packet = AuthSwitchRequestPacket.readPacket(payloadBuf);
        } else if (ErrorPacket.isErrorPacket(payloadBuf)) {
            packet = ErrorPacket.readPacket(payloadBuf);
        } else {
            packet = RawPacket.readPacket(payloadBuf, obtainHandshakeV10Packet().getServerVersion());
        }
        return packet;
    }

    private Mono<Void> handleAuthResponse(ByteBuf payloadBuf, AuthenticationPlugin plugin
            , Map<String, AuthenticationPlugin> pluginMap) {

        Mono<Void> mono;
        if (OkPacket.isOkPacket(payloadBuf)) {
            OkPacket packet = OkPacket.readPacket(payloadBuf, obtainHandshakeV10Packet().getCapabilityFlags());
            setServerStatus(packet.getStatusFags(), true);
            mono = Mono.empty();
        } else if (ErrorPacket.isErrorPacket(payloadBuf)) {
            mono = closeConnection()
                    .then(Mono.error(new JdbdMySQLException("auth error, %s", ErrorPacket.readPacket(payloadBuf))));
        } else if (this.authCounter.addAndGet(1) >= 100) {
            mono = Mono.error(new JdbdMySQLException("TooManyAuthenticationPluginNegotiations"));
        } else {
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
            mono = Flux.fromIterable(authPlugin.nextAuthenticationStep(payloadBuf))
                    .map(this::convertPayloadBufToPacketBuf)
                    // all packet send to server
                    .flatMap(this::sendPacket)
                    .then(Mono.defer(this::receivePayload))
                    // recursion invoke handleAuthResponse
                    .flatMap(buffer -> handleAuthResponse(buffer, authPlugin, pluginMap));
        }
        return mono;
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

    private void handleHandshakeV10Packet(HandshakeV10Packet packet) {
        if (!this.handshakeV10Packet.compareAndSet(null, packet)) {
            throw new IllegalStateException("handshakeV10Packet isn't null.");
        }
        this.clientCollationIndex.compareAndSet(null, mapClientCollationIndex());
    }


    private Mono<HandshakeV10Packet> receiveHandshakeV10Packet(ByteBuf packetBuf) {
        if (ErrorPacket.isErrorPacket(packetBuf)) {
            // reject packet
            ErrorPacket errorPacket = ErrorPacket.readPacketAtHandshake(packetBuf);
            return FutureMono.from(this.connection.channel().close())
                    .then(Mono.defer(() ->
                            Mono.error(new JdbdMySQLException(
                                    "handshake packet is error packet,close connection.ErrorPacket[%s]", errorPacket))
                    ));
        }
        return Mono.just(HandshakeV10Packet.readHandshake(packetBuf));
    }

    /**
     * @return read-only {@link ByteBuf}
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private Mono<ByteBuf> createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.clientCharset;
        final int clientFlag = obtainClientCapability();

        final ByteBuf packetBuffer = createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, obtainClientCollationIndex());
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

    private Mono<ByteBuf> createSslRequestPayload() {
        ByteBuf packetBuf = createPacketBuffer(32);
        // 1. client_flag
        PacketUtils.writeInt4(packetBuf, obtainClientCapability());
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuf, MAX_PACKET_SIZE);
        // 3. character_set,
        PacketUtils.writeInt1(packetBuf, obtainClientCollationIndex());
        // 4. filler
        packetBuf.writeZero(23);
        return Mono.just(packetBuf);
    }

    private Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = loadAuthenticationPluginMap();

        Properties properties = this.properties;
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        String pluginName = handshakeV10Packet.getAuthPluginName();

        AuthenticationPlugin plugin = pluginMap.get(pluginName);
        boolean skipPassword = false;
        final boolean useSsl = this.useSsl.get();
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

    private int obtainClientCapability() {
        Integer clientCapability = this.clientCapability.get();
        if (clientCapability == null) {
            clientCapability = createClientCapability();
            this.clientCapability.set(clientCapability);
        }
        return clientCapability;
    }

    private int createClientCapability() {
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        final int serverFlag = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = MySQLStringUtils.hasText(this.mySQLUrl.getOriginalDatabase())
                && !env.getRequiredProperty(PropertyKey.createDatabaseIfNotExist, Boolean.class);

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

                | (NONE.equals(env.getProperty(PropertyKey.connectionAttributes)) ? 0 : (serverFlag & CLIENT_CONNECT_ATTRS))
                | (env.getRequiredProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class) != PropertyDefinitions.SslMode.DISABLED ? (serverFlag & CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }

    private byte obtainClientCollationIndex() {
        Byte b = this.clientCollationIndex.get();
        if (b == null) {
            throw new IllegalStateException("client no handshake");
        }
        return b;
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
        AbstractHandshakePacket handshakePacket = this.handshakeV10Packet.get();
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

    /**
     * packet header have read.
     *
     * @see #readPacketHeader(ByteBuf)
     * @see #sendPacket(ByteBuf)
     */
    private Mono<ByteBuf> receivePayload() {
        return this.packetReceiver.receiveOne()
                .doOnNext(this::readPacketHeader);
    }

    /**
     * @see #receivePayload()
     * @see #sendPacket(ByteBuf)
     */
    private void readPacketHeader(ByteBuf packetBuf) {
        packetBuf.skipBytes(3);
        this.sequenceId.set(PacketUtils.readInt1(packetBuf));
    }

    /**
     * @see #readPacketHeader(ByteBuf)
     * @see #receivePayload()
     */
    private Mono<Void> sendPacket(ByteBuf packetBuffer) {
        PacketUtils.writePacketHeader(packetBuffer, this.sequenceId.addAndGet(1));
        return Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuffer)));
    }

    private Mono<Void> closeConnection() {
        return FutureMono.from(this.connection.channel().close());
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

    private static void appendBuildInPluginClassNameList(List<String> pluginClassNameList) {
        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());
        pluginClassNameList.add(MySQLClearPasswordPlugin.class.getName());
        pluginClassNameList.add(Sha256PasswordPlugin.class.getName());
        pluginClassNameList.add(CachingSha2PasswordPlugin.class.getName());

        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());

    }


}
