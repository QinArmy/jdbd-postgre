package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.authentication.Sha256PasswordPlugin;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLHost0;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.SessionCloseException;
import io.jdbd.vendor.env.HostInfo;
import io.jdbd.vendor.env.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.ConnectionTask;
import io.jdbd.vendor.task.SslWrapper;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;
import io.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html">Connection Phase</a>
 */
final class MySQLConnectionTask extends CommunicationTask implements AuthenticateAssistant
        , ConnectionTask {

    static Mono<AuthenticateResult> authenticate(TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                MySQLConnectionTask task = new MySQLConnectionTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLConnectionTask.class);

    private final TaskAdjutant adjutant;

    private final MonoSink<AuthenticateResult> sink;

    private final Map<String, AuthenticationPlugin> pluginMap;

    private final MySQLHost0 hostInfo;

    private final Properties properties;

    private Charset handshakeCharset;

    private byte handshakeCollationIndex;

    private int sequenceId = -1;

    private Phase phase;

    private int capability = 0;

    private Handshake10 handshake;

    private AuthenticationPlugin plugin;

    // non-volatile ,because all modify in netty EventLoop .
    private int authCounter = 0;

    private String errorMessage;

    private Consumer<SslWrapper> sslConsumer;

    private MySQLConnectionTask(TaskAdjutant adjutant, MonoSink<AuthenticateResult> sink) {
        super(adjutant, sink::error);
        this.sink = sink;

        this.adjutant = adjutant;
        this.hostInfo = adjutant.host();
        this.properties = this.hostInfo.getProperties();
        this.pluginMap = loadAuthenticationPluginMap();

        Charset charset = this.properties.get(MyKey.characterEncoding, Charset.class);
        if (charset == null || !Charsets.isSupportCharsetClient(charset)) {
            charset = StandardCharsets.UTF_8;
        }
        this.handshakeCharset = charset;
    }

    /*################################## blow AuthenticateAssistant method ##################################*/

    @Override
    public Charset getHandshakeCharset() {
        return this.handshakeCharset;
    }

    @Override
    public Charset getPasswordCharset() {
        String pwdCharset = this.properties.get(MyKey.passwordCharacterEncoding);
        return pwdCharset == null ? this.handshakeCharset : Charset.forName(pwdCharset);
    }

    @Override
    public HostInfo getHostInfo() {
        return this.hostInfo;
    }

    @Override
    public boolean isUseSsl() {
        return Capabilities.supportSsl(this.capability);
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return this.adjutant.createPacketBuffer(initialPayloadCapacity);
    }


    @Override
    public MySQLServerVersion getServerVersion() {
        return this.handshake.getServerVersion();
    }

    @Override
    public ByteBufAllocator allocator() {
        return this.adjutant.allocator();
    }

    @Override
    public void addSsl(Consumer<SslWrapper> sslConsumer) {
        this.sslConsumer = sslConsumer;
    }

    @Override
    public final boolean reconnect() {
        return false;
    }

    @Override
    public boolean disconnect() {
        return this.phase == Phase.DISCONNECT;
    }

    @Override
    public String toString() {
        String text;
        text = super.toString();
        if (this.errorMessage != null) {
            text = text + "##" + this.errorMessage;
        }
        return text;
    }

    /*################################## blow protected method ##################################*/

    @Nullable
    @Override
    protected Publisher<ByteBuf> start() {
        if (this.phase == null) {
            //may be load plugin occur error.
            this.phase = Phase.RECEIVE_HANDSHAKE;
        }
        // no data to send after receive Handshake packet from server.
        return null;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false, continueDecode = Packets.hasOnePacket(cumulateBuffer);
        while (continueDecode) {
            switch (this.phase) {
                case DISCONNECT: {
                    taskEnd = true;
                    continueDecode = false;
                }
                break;
                case RECEIVE_HANDSHAKE: {
                    receiveHandshakeAndSendResponse(cumulateBuffer);
                    taskEnd = this.phase == Phase.DISCONNECT;
                    continueDecode = false;
                }
                break;
                case HANDSHAKE_RESPONSE: {
                    this.phase = Phase.AUTHENTICATE;
                    taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case AUTHENTICATE: {
                    taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                default:
                    throw new IllegalStateException(String.format("%s this.phase[%s] error.", this, this.phase));
            }
        }
        if (taskEnd && this.phase != Phase.DISCONNECT) {
            this.phase = Phase.END;
        }
        return taskEnd;
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.phase != Phase.END) {
            this.sink.error(MySQLExceptions.wrap(e));
        }
        return Action.TASK_END;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.DISCONNECT) {
            handleAuthenticateFailure(new SessionCloseException("Channel unexpected close."));
        }
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void receiveHandshakeAndSendResponse(final ByteBuf cumulateBuffer) {
        //1. read handshake packet
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final Handshake10 handshake;
        handshake = Handshake10.readHandshake(cumulateBuffer.readSlice(payloadLength));
        this.handshake = handshake;
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive handshake success:\n{}", handshake);
        }
        //2. negotiate capabilities
        final int negotiatedCapability = createNegotiatedCapability(handshake);
        this.capability = negotiatedCapability;

        //3.create handshake collation index and charset
        int handshakeCollationIndex;
        handshakeCollationIndex = Charsets.getCollationIndexForJavaEncoding(
                this.handshakeCharset.name(), handshake.getServerVersion());
        if (handshakeCollationIndex == 0) {
            handshakeCollationIndex = Charsets.MYSQL_COLLATION_INDEX_utf8mb4;
            this.handshakeCharset = StandardCharsets.UTF_8;
        }
        this.handshakeCollationIndex = (byte) handshakeCollationIndex;

        //4. optional ssl request or send plaintext handshake response.
        if (Capabilities.supportSsl(negotiatedCapability)) {
            LOG.debug("send ssl request.");
            sendSslRequest();
        } else {
            LOG.debug("plaintext send handshake response.");
        }
        this.packetPublisher = createHandshakeResponsePacket();
        this.phase = Phase.HANDSHAKE_RESPONSE;
    }

    /**
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean authenticateDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverConsumer) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("decode authenticate packet ,authCounter:{}", this.authCounter);
        }
        assertPhase(Phase.AUTHENTICATE);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final ByteBuf payload = cumulateBuffer.readSlice(payloadLength);
        boolean taskEnd;
        if (++this.authCounter > 100) {
            JdbdSQLException e = new JdbdSQLException(new SQLException("TooManyAuthenticationPluginNegotiations"
                    , SQLStates.CONNECTION_EXCEPTION));
            handleAuthenticateFailure(e);
            taskEnd = true;
        } else if (OkPacket.isOkPacket(payload)) {
            OkPacket ok = OkPacket.read(payload, this.capability);
            serverConsumer.accept(ok.getStatusFags());
            LOG.debug("MySQL authentication success,info:{}", ok.getInfo());
            this.sink.success(new AuthenticateResult(this.handshake, this.capability));
            taskEnd = true;
        } else if (ErrorPacket.isErrorPacket(payload)) {
            ErrorPacket error;
            if (this.sequenceId < 2) {
                error = ErrorPacket.read(payload, 0, obtainServerCharset());
            } else {
                error = ErrorPacket.read(payload, this.capability, obtainServerCharset());
            }
            taskEnd = true;
            handleAuthenticateFailure(MySQLExceptions.createErrorPacketException(error));
        } else {
            try {
                taskEnd = processNextAuthenticationNegotiation(payload);
            } catch (Throwable e) {
                taskEnd = true;
                handleAuthenticateFailure(MySQLExceptions.wrap(e));
            }
        }
        return taskEnd;
    }


    private int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) & 0xFF;
        this.sequenceId = sequenceId;
        return sequenceId;
    }

    private void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId & 0xFF;
    }


    /**
     * @see #receiveHandshakeAndSendResponse(ByteBuf)
     */
    @Nullable
    private Mono<ByteBuf> createHandshakeResponsePacket() {
        Mono<ByteBuf> mono;

        try {
            Pair<AuthenticationPlugin, Boolean> pair = obtainAuthenticationPlugin();
            AuthenticationPlugin plugin = pair.getFirst();
            this.plugin = plugin;
            ByteBuf pluginOut = createAuthenticationDataFor41(plugin, pair.getSecond());
            final ByteBuf packet;
            packet = createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut);
            LOG.debug("readableBytes:{},length:{}", packet.readableBytes(), Packets.getInt3(packet, 0));
            mono = Mono.just(packet);
        } catch (Throwable e) {
            JdbdException je = MySQLExceptions.wrap(e);
            handleAuthenticateFailure(je);
            mono = null;
        }
        return mono;
    }

    /**
     * @see #receiveHandshakeAndSendResponse(ByteBuf)
     */
    private void sendSslRequest() {
        try {
            Object sslObject = ReactorSslProviderBuilder.builder()
                    .allocator(this.adjutant.allocator())
                    .hostInfo(this.hostInfo)
                    .serverVersion(this.handshake.getServerVersion())
                    .buildSslHandler();

            // add sslHandler to channel line.
            Objects.requireNonNull(this.sslConsumer, "this.sslConsumer")
                    .accept(SslWrapper.create(this, createSendSSlRequestPacket(), sslObject));
            if (LOG.isDebugEnabled()) {
                LOG.debug("add {} to ChannelPipeline complete.", SslHandler.class.getName());
            }
        } catch (SQLException e) {
            handleAuthenticateFailure(MySQLExceptions.wrap(e));
        }
    }

    /**
     * @see #sendSslRequest()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html">Protocol::SSLRequest</a>
     */
    private Mono<ByteBuf> createSendSSlRequestPacket() {
        ByteBuf packet = this.adjutant.createPacketBuffer(32);
        // 1. client_flag
        Packets.writeInt4(packet, this.capability);
        // 2. max_packet_size
        Packets.writeInt4(packet, this.adjutant.host().maxAllowedPayload());
        // 3.handshake character_set,
        packet.writeByte(this.handshakeCollationIndex);
        // 4. filler
        packet.writeZero(23);
        Packets.writeHeader(packet, addAndGetSequenceId());
        return Mono.just(packet);
    }


    /**
     * @return true : task end.
     * @see #authenticateDecode(ByteBuf, Consumer)
     */
    private boolean processNextAuthenticationNegotiation(final ByteBuf payload) {

        final AuthenticationPlugin authPlugin;
        if (Packets.isAuthSwitchRequestPacket(payload)) {
            payload.skipBytes(1); // skip type header
            String pluginName = Packets.readStringTerm(payload, StandardCharsets.US_ASCII);
            LOG.debug("Auth switch request method[{}]", pluginName);
            if (this.plugin.getProtocolPluginName().equals(pluginName)) {
                authPlugin = this.plugin;
            } else {
                authPlugin = this.pluginMap.get(pluginName);
                if (authPlugin == null) {
                    String message = String.format("BadAuthenticationPlugin[%s] from server,please check %s %s %s three properties."
                            , pluginName, MyKey.disabledAuthenticationPlugins
                            , MyKey.authenticationPlugins, MyKey.defaultAuthenticationPlugin);
                    handleAuthenticateFailure(new MySQLJdbdException(message));
                    return true;
                }
            }
            authPlugin.reset();
        } else {
            authPlugin = this.plugin;
            payload.skipBytes(1); // skip type header
        }
        List<ByteBuf> outputList = authPlugin.nextAuthenticationStep(payload);
        if (!outputList.isEmpty()) {
            // plugin auth
            List<ByteBuf> packetList = new ArrayList<>(outputList.size());
            for (ByteBuf authPayload : outputList) {
                packetList.add(writeAuthPayload(authPayload));
            }
            this.packetPublisher = Flux.fromIterable(packetList);
        }
        return false;
    }

    /**
     * @see #processNextAuthenticationNegotiation(ByteBuf)
     */
    private ByteBuf writeAuthPayload(final ByteBuf payload) {
        final ByteBuf packet;
        final int readableBytes = payload.readableBytes();
        if (readableBytes < Packets.MAX_PAYLOAD) {
            packet = this.adjutant.allocator().buffer(Packets.HEADER_SIZE + readableBytes);

            Packets.writeInt3(packet, readableBytes);
            packet.writeByte(addAndGetSequenceId());
            packet.writeBytes(payload);

            payload.release(); // release payload.
        } else {
            // no bug ,never here.
            throw new MySQLJdbdException("%s send too long auth data.", this.plugin);
        }
        return packet;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private ByteBuf createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = this.capability;

        final ByteBuf packetBuffer = this.adjutant.createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        Packets.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        Packets.writeInt4(packetBuffer, this.adjutant.host().maxAllowedPayload());
        // 3. character_set
        Packets.writeInt1(packetBuffer, this.handshakeCollationIndex);
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        Packets.writeStringTerm(packetBuffer, this.hostInfo.getUser().getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            Packets.writeStringLenEnc(packetBuffer, pluginOut);
        } else {
            packetBuffer.writeByte(pluginOut.readableBytes());
            packetBuffer.writeBytes(pluginOut);
        }
        pluginOut.release();

        // 7. database
        if ((clientFlag & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.hostInfo.getDbName();
            if (!MySQLStrings.hasText(database)) {
                throw new MySQLJdbdException("client flag error,check this.getClientFlat() method.");
            }
            Packets.writeStringTerm(packetBuffer, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            Packets.writeStringTerm(packetBuffer, authPluginName.getBytes(clientCharset));
        }
        // 9. client connection attributes
        if ((clientFlag & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {
            Map<String, String> propertySource = createConnectionAttributes();
            // length of all key-values,affected rows
            Packets.writeIntLenEnc(packetBuffer, propertySource.size());
            for (Map.Entry<String, String> e : propertySource.entrySet()) {
                // write key
                Packets.writeStringLenEnc(packetBuffer, e.getKey().getBytes(clientCharset));
                // write value
                Packets.writeStringLenEnc(packetBuffer, e.getValue().getBytes(clientCharset));
            }

        }
        //TODO 10.zstd_compression_level,compression level for zstd compression algorithm
        //packetBuffer.writeByte(0);
        Packets.writeHeader(packetBuffer, addAndGetSequenceId());
        return packetBuffer;
    }

    private ByteBuf createAuthenticationDataFor41(AuthenticationPlugin plugin, boolean skipPassword) {
        ByteBuf payloadBuf;
        if (skipPassword) {
            // skip password
            payloadBuf = Unpooled.EMPTY_BUFFER;
        } else {
            Handshake10 handshake10 = this.handshake;
            String seed = handshake10.getPluginSeed();
            byte[] seedBytes = seed.getBytes();
            ByteBuf fromServer = this.adjutant.allocator().buffer(seedBytes.length);
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

    /**
     * @see #loadAuthenticationPluginMap()
     * @see #authenticateDecode(ByteBuf, Consumer)
     * @see #onChannelClose()
     * @see #processNextAuthenticationNegotiation(ByteBuf)
     */
    private void handleAuthenticateFailure(JdbdException e) {
        this.phase = Phase.DISCONNECT;
        this.errorMessage = e.getMessage();
        this.sink.error(e);
    }


    /**
     * @see #createHandshakeResponsePacket()
     */
    private Pair<AuthenticationPlugin, Boolean> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = this.pluginMap;

        Properties properties = this.properties;
        String pluginName = this.handshake.getAuthPluginName();

        AuthenticationPlugin plugin = pluginMap.get(pluginName);
        boolean skipPassword = false;
        final boolean useSsl = isUseSsl();
        if (plugin == null) {
            plugin = pluginMap.get(PluginUtils.getDefaultMechanism(properties));
        } else if (Sha256PasswordPlugin.PLUGIN_NAME.equals(pluginName)
                && !useSsl
                && properties.get(MyKey.serverRSAPublicKeyFile) == null
                && !properties.getOrDefault(MyKey.allowPublicKeyRetrieval, Boolean.class)) {
            /*
             * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
             * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
             * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
             * sent to avoid subjecting it to lesser security levels.
             */
            String defaultPluginName = properties.getOrDefault(MyKey.defaultAuthenticationPlugin);
            skipPassword = !pluginName.equals(defaultPluginName);
            plugin = pluginMap.get(defaultPluginName);
        }
        if (plugin.requiresConfidentiality() && !useSsl) {
            throw new MySQLJdbdException("AuthenticationPlugin[%s] required SSL", plugin.getClass().getName());
        }
        return new Pair<>(plugin, skipPassword);
    }


    private Map<String, String> createConnectionAttributes() {
        String connectionStr = this.properties.get(MyKey.connectionAttributes);
        Map<String, String> attMap = new HashMap<>();

        if (connectionStr != null) {
            String[] pairArray = connectionStr.split(",");
            for (String pair : pairArray) {
                String[] kv = pair.split(":");
                if (kv.length != 2) {
                    throw new IllegalStateException(String.format("key[%s] can't resolve pair." +
                            "", MyKey.connectionAttributes));
                }
                attMap.put(kv[0].trim(), kv[1].trim());
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        String clientVersion = ClientProtocol0.class.getPackage().getImplementationVersion();
        if (clientVersion == null) {
            clientVersion = "jdbd-mysql-test";
        }
        attMap.put("_client_name", "JDBD-MySQL");
        attMap.put("_client_version", clientVersion);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_client_license", "apache");
        return attMap;
    }


    private Charset obtainServerCharset() {
        Charset charset = Charsets.getJavaCharsetByCollationIndex(this.handshake.getCollationIndex());
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        return charset;
    }


    private int createNegotiatedCapability(final Handshake10 handshake) {
        final int serverCapability = handshake.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = MySQLStrings.hasText(this.hostInfo.getDbName())
                && !env.getOrDefault(MyKey.createDatabaseIfNotExist, Boolean.class);

        // Servers between 8.0.23 8.0.25 are affected by Bug#103102, Bug#103268 and Bug#103377. Query attributes cannot be sent to these servers.
        final boolean supportQueryAttr = handshake.getServerVersion().meetsMinimum(MySQLServerVersion.V8_0_26);

        return Capabilities.CLIENT_SECURE_CONNECTION
                | Capabilities.CLIENT_PLUGIN_AUTH
                | (serverCapability & Capabilities.CLIENT_LONG_PASSWORD)  //
                | (serverCapability & Capabilities.CLIENT_PROTOCOL_41)    //

                | (serverCapability & Capabilities.CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & Capabilities.CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & Capabilities.CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & Capabilities.CLIENT_LONG_FLAG)      //

                | (serverCapability & Capabilities.CLIENT_DEPRECATE_EOF)  //
                | (serverCapability & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getOrDefault(MyKey.useCompression, Boolean.class) ? (serverCapability & Capabilities.CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & Capabilities.CLIENT_CONNECT_WITH_DB) : 0)

                | (env.getOrDefault(MyKey.useAffectedRows, Boolean.class) ? 0 : (serverCapability & Capabilities.CLIENT_FOUND_ROWS))
                | (env.getOrDefault(MyKey.allowLoadLocalInfile, Boolean.class) ? (serverCapability & Capabilities.CLIENT_LOCAL_FILES) : 0)
                | (env.getOrDefault(MyKey.interactiveClient, Boolean.class) ? (serverCapability & Capabilities.CLIENT_INTERACTIVE) : 0)
                | (serverCapability & Capabilities.CLIENT_MULTI_STATEMENTS)

                | (env.getOrDefault(MyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverCapability & Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))
                | (Constants.NONE.equals(env.get(MyKey.connectionAttributes)) ? 0 : (serverCapability & Capabilities.CLIENT_CONNECT_ATTRS))
                | (env.getOrDefault(MyKey.sslMode, Enums.SslMode.class) != Enums.SslMode.DISABLED ? (serverCapability & Capabilities.CLIENT_SSL) : 0)
                | (serverCapability & Capabilities.CLIENT_SESSION_TRACK) // TODO ZORO MYSQLCONNJ-437?

                | (/*supportQueryAttr ? (serverCapability & Capabilities.CLIENT_QUERY_ATTRIBUTES) :*/ 0)
                ;
    }

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }

    /**
     * @return a unmodifiable map ,key : {@link AuthenticationPlugin#getProtocolPluginName()}.
     */
    private Map<String, AuthenticationPlugin> loadAuthenticationPluginMap() {
        Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap = this.adjutant.obtainPluginMechanismMap();
        Map<String, AuthenticationPlugin> map = new HashMap<>((int) (pluginClassMap.size() / 0.75F));
        for (Map.Entry<String, Class<? extends AuthenticationPlugin>> e : pluginClassMap.entrySet()) {
            map.put(e.getKey(), loadPlugin(e.getValue(), this));
        }
        map = MySQLCollections.unmodifiableMap(map);
        return map;
    }



    /*################################## blow private static method ##################################*/

    /**
     * @see #loadAuthenticationPluginMap()
     */
    private static AuthenticationPlugin loadPlugin(Class<? extends AuthenticationPlugin> pluginClass
            , AuthenticateAssistant assistant) {
        try {

            Method method = pluginClass.getDeclaredMethod("getInstance", AuthenticateAssistant.class);
            if (!pluginClass.isAssignableFrom(method.getReturnType())) {
                String message = String.format("plugin[%s] getInstance return error type.", pluginClass.getName());
                throw new MySQLJdbdException(message);
            }
            if (!Modifier.isStatic(method.getModifiers())) {
                String message = String.format("plugin[%s] getInstance method isn't static.", pluginClass.getName());
                throw new MySQLJdbdException(message);
            }
            return (AuthenticationPlugin) method.invoke(null, assistant);
        } catch (NoSuchMethodException e) {
            String message = String.format("plugin[%s] no getInstance() factory method.", pluginClass.getName());
            throw new MySQLJdbdException(e, message);
        } catch (IllegalAccessException | InvocationTargetException e) {
            String message = String.format("plugin[%s] getInstance() invoke error.", pluginClass.getName());
            throw new MySQLJdbdException(e, message);
        } catch (Throwable e) {
            String message = String.format("load plugin[%s] occur error.", pluginClass.getName());
            throw new MySQLJdbdException(e, message);
        }
    }





    /*################################## blow private static method ##################################*/


    private enum Phase {
        RECEIVE_HANDSHAKE,
        HANDSHAKE_RESPONSE,
        AUTHENTICATE,
        DISCONNECT,
        END
    }


}
