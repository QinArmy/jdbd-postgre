package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.PropertyException;
import io.jdbd.SessionCloseException;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.AbstractCommunicationTask;
import io.jdbd.vendor.task.ConnectionTask;
import io.jdbd.vendor.task.MorePacketSignal;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html">Connection Phase</a>
 */
final class MySQLConnectionTask extends AbstractCommunicationTask implements AuthenticateAssistant
        , ConnectionTask, MySQLTask {

    static Mono<AuthenticateResult> authenticate(MySQLTaskAdjutant adjutant) {
        return Mono.create(sink ->
                new MySQLConnectionTask(adjutant, sink)
                        .submit(sink::error)

        );
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLConnectionTask.class);

    private static final Map<String, String> PLUGIN_NAME_MAP = createPluginNameMap();

    private static final List<String> PLUGIN_NAME_LIST = createPluginNameList();

    private final MySQLTaskAdjutant adjutant;

    private final MonoSink<AuthenticateResult> sink;

    private final Map<String, AuthenticationPlugin> pluginMap;

    private final HostInfo<PropertyKey> hostInfo;

    private final Properties<PropertyKey> properties;

    private Charset handshakeCharset;

    private byte handshakeCollationIndex;

    private int sequenceId = -1;

    private Phase phase;

    private int negotiatedCapability = 0;

    private HandshakeV10Packet handshake;

    private AuthenticationPlugin plugin;

    // non-volatile ,because all modify in netty EventLoop .
    private int authCounter = 0;

    private Consumer<Object> sslConsumer;

    private Consumer<Void> disconnectConsumer;

    private MySQLConnectionTask(MySQLTaskAdjutant adjutant, MonoSink<AuthenticateResult> sink) {
        super(adjutant);

        this.adjutant = adjutant;
        this.hostInfo = adjutant.obtainHostInfo();
        this.properties = this.hostInfo.getProperties();
        this.pluginMap = loadAuthenticationPluginMap();

        this.sink = sink;
        this.handshakeCharset = this.properties.getProperty(PropertyKey.characterEncoding
                , Charset.class, StandardCharsets.UTF_8);
    }

    /*################################## blow AuthenticateAssistant method ##################################*/

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
    public HostInfo<PropertyKey> getHostInfo() {
        return this.hostInfo;
    }

    @Override
    public boolean isUseSsl() {
        return Capabilities.supportSsl(this.negotiatedCapability);
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return this.adjutant.createPacketBuffer(initialPayloadCapacity);
    }


    @Override
    public ServerVersion getServerVersion() {
        return this.handshake.getServerVersion();
    }

    @Override
    public ByteBufAllocator allocator() {
        return this.adjutant.allocator();
    }

    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher != null) {
            this.packetPublisher = null;
        }
        return publisher;
    }

    @Override
    public void connectSignal(Consumer<Object> sslConsumer, Consumer<Void> disconnectConsumer) {
        this.sslConsumer = sslConsumer;
        this.disconnectConsumer = disconnectConsumer;
    }

    /*################################## blow protected method ##################################*/

    @Nullable
    @Override
    protected Publisher<ByteBuf> internalStart(final MorePacketSignal signal) {
        if (this.phase == null) {
            // maybe load plugin occur error.
            this.phase = Phase.RECEIVE_HANDSHAKE;
        }
        // no data to send after receive Handshake packet from server.
        return null;
    }


    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        boolean taskEnd = false;
        switch (this.phase) {
            case LOAD_PLUGIN_ERROR: {
                taskEnd = true;
            }
            break;
            case RECEIVE_HANDSHAKE: {
                //1. read handshake packet
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                HandshakeV10Packet handshake;
                handshake = HandshakeV10Packet.readHandshake(cumulateBuffer.readSlice(payloadLength));
                this.handshake = handshake;
                LOG.debug("receive handshake success:\n{}", handshake);

                //2. negotiate capabilities
                final int negotiatedCapability = createNegotiatedCapability(handshake);
                this.negotiatedCapability = negotiatedCapability;

                //3.create handshake collation index and charset
                final byte handshakeCollationIndex;
                handshakeCollationIndex = CharsetMapping.getHandshakeCollationIndex(
                        this.handshakeCharset, this.handshake.getServerVersion());
                this.handshakeCollationIndex = handshakeCollationIndex;
                if (handshakeCollationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_utf8) {
                    this.handshakeCharset = StandardCharsets.UTF_8;
                }

                //4. optional ssl request or send plaintext handshake response.
                if (Capabilities.supportSsl(negotiatedCapability)) {
                    this.packetPublisher = createSendSSlRequestPacket(negotiatedCapability, handshakeCollationIndex);
                    this.phase = Phase.SSL_REQUEST;
                } else {
                    LOG.debug("plaintext send handshake response.");
                    this.packetPublisher = Mono.just(createHandshakeResponsePacket());
                    this.phase = Phase.HANDSHAKE_RESPONSE;
                }
            }
            break;
            case HANDSHAKE_RESPONSE: {
                this.phase = Phase.AUTHENTICATE;
                taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
            }
            break;
            case AUTHENTICATE: {
                taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default:
                throw new IllegalStateException(String.format("%s this.phase[%s] error.", this, this.phase));
        }
        return taskEnd;
    }

    /**
     * <p>
     * maybe modify {@link #phase}
     * </p>
     *
     * @see #onSendSuccess()
     */
    @Override
    protected boolean internalOnSendSuccess() {
        boolean hasMorePacket = false;
        switch (this.phase) {
            case SSL_REQUEST: {
                //  maybe modify this.phase.
                hasMorePacket = handleSslRequestSendSuccess();
            }
            break;
            case HANDSHAKE_RESPONSE: {
                LOG.trace("handshake response send success.");
            }
            break;
            case AUTHENTICATE: {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("send authenticate packet success,authCounter={} .", this.authCounter);
                }
            }
            break;
            default:
        }
        return hasMorePacket;
    }


    @Nullable
    @Override
    protected Publisher<ByteBuf> internalError(Throwable e) {
        this.sink.error(MySQLExceptions.wrap(e));
        return null;
    }

    @Override
    protected void internalOnChannelClose() {
        if (this.phase != Phase.DISCONNECT) {
            this.sink.error(new SessionCloseException("Channel unexpected close."));
        }
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean authenticateDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverConsumer) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("decode authenticate packet ,authCounter:{}", this.authCounter);
        }
        assertPhase(Phase.AUTHENTICATE);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int payloadStartIndex = cumulateBuffer.readerIndex();
        boolean taskEnd;
        if (++this.authCounter > 100) {
            this.sink.error(new JdbdSQLException(new SQLException("TooManyAuthenticationPluginNegotiations"
                    , SQLStates.CONNECTION_EXCEPTION)));
            taskEnd = true;
        } else if (OkPacket.isOkPacket(cumulateBuffer)) {
            OkPacket ok = OkPacket.read(cumulateBuffer, this.negotiatedCapability);
            serverConsumer.accept(ok.getStatusFags());
            LOG.debug("MySQL authentication success,info:{}", ok.getInfo());
            this.sink.success(new AuthenticateResult(this.handshake, this.negotiatedCapability));
            taskEnd = true;
        } else if (ErrorPacket.isErrorPacket(cumulateBuffer)) {
            ErrorPacket error;
            if (this.sequenceId < 2) {
                error = ErrorPacket.readPacket(cumulateBuffer, 0, obtainServerCharset());
            } else {
                error = ErrorPacket.readPacket(cumulateBuffer, this.negotiatedCapability, obtainServerCharset());
            }
            this.sink.error(MySQLExceptions.createErrorPacketException(error));
            taskEnd = true;
        } else {
            taskEnd = processNextAuthenticationNegotiation(cumulateBuffer);
        }
        cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);
        return taskEnd;
    }


    /**
     * <p>
     * maybe modify {@link #phase}
     * </p>
     *
     * @return true: has more packet send.
     * @see #internalOnSendSuccess()
     */
    private boolean handleSslRequestSendSuccess() {
        LOG.debug("ssl request send success.");
        try {
            Object sslObject = ReactorSslProviderBuilder.builder()
                    .allocator(this.adjutant.allocator())
                    .hostInfo(this.hostInfo)
                    .serverVersion(this.handshake.getServerVersion())
                    .buildSslHandler();

            // add sslHandler to channel line.
            Objects.requireNonNull(this.sslConsumer, "this.sslConsumer")
                    .accept(sslObject);

            if (LOG.isDebugEnabled()) {
                LOG.debug("add {} to ChannelPipeline complete.", SslHandler.class.getName());
            }
        } catch (SQLException e) {
            this.phase = Phase.DISCONNECT;
            this.sink.error(MySQLExceptions.wrap(e));
            // notify TaskExecutor immediately disconnect.
            Objects.requireNonNull(this.disconnectConsumer, "this.disconnectConsumer")
                    .accept(null);
            return false;
        }
        this.packetPublisher = Mono.just(createHandshakeResponsePacket());
        this.phase = Phase.HANDSHAKE_RESPONSE;
        return true;
    }

    private int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) & 0XFF;
        this.sequenceId = sequenceId;
        return sequenceId;
    }

    private void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId & 0XFF;
    }


    /**
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #handleSslRequestSendSuccess()
     */
    private ByteBuf createHandshakeResponsePacket() {
        Pair<AuthenticationPlugin, Boolean> pair = obtainAuthenticationPlugin();
        AuthenticationPlugin plugin = pair.getFirst();
        this.plugin = plugin;
        ByteBuf pluginOut = createAuthenticationDataFor41(plugin, pair.getSecond());
        return createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut);
    }

    /**
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html">Protocol::SSLRequest</a>
     */
    private Mono<ByteBuf> createSendSSlRequestPacket(final int negotiatedCapability, final byte handshakeCollationIndex) {
        ByteBuf packet = this.adjutant.createPacketBuffer(32);
        // 1. client_flag
        PacketUtils.writeInt4(packet, negotiatedCapability);
        // 2. max_packet_size
        PacketUtils.writeInt4(packet, PacketUtils.MAX_PAYLOAD);
        // 3.handshake character_set,
        packet.writeByte(handshakeCollationIndex);
        // 4. filler
        packet.writeZero(23);
        PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
        return Mono.just(packet);
    }


    /**
     * @see #authenticateDecode(ByteBuf, Consumer)
     */
    private boolean processNextAuthenticationNegotiation(ByteBuf cumulateBuffer) {

        final AuthenticationPlugin authPlugin;
        if (PacketUtils.isAuthSwitchRequestPacket(cumulateBuffer)) {
            cumulateBuffer.skipBytes(1); // skip type header
            String pluginName = PacketUtils.readStringTerm(cumulateBuffer, StandardCharsets.US_ASCII);
            LOG.debug("Auth switch request method[{}]", pluginName);
            if (this.plugin.getProtocolPluginName().equals(pluginName)) {
                authPlugin = this.plugin;
            } else {
                authPlugin = this.pluginMap.get(pluginName);
                if (authPlugin == null) {
                    this.sink.error(new MySQLJdbdException("BadAuthenticationPlugin[%s] from server.", pluginName));
                    return true;
                }
            }
            authPlugin.reset();
        } else {
            authPlugin = this.plugin;
            cumulateBuffer.skipBytes(1); // skip type header
        }
        List<ByteBuf> outputList = authPlugin.nextAuthenticationStep(cumulateBuffer);
        if (!outputList.isEmpty()) {
            // plugin auth
            this.packetPublisher = Flux.fromIterable(outputList);
        }
        return false;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private ByteBuf createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = this.negotiatedCapability;

        final ByteBuf packetBuffer = this.adjutant.createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, ClientProtocol.MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, this.handshakeCollationIndex);
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        PacketUtils.writeStringTerm(packetBuffer, this.hostInfo.getUser().getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & ClientProtocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            PacketUtils.writeStringLenEnc(packetBuffer, pluginOut);
        } else {
            packetBuffer.writeByte(pluginOut.readableBytes());
            packetBuffer.writeBytes(pluginOut);
        }
        pluginOut.release();

        // 7. database
        if ((clientFlag & ClientProtocol.CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.hostInfo.getDbName();
            if (!MySQLStringUtils.hasText(database)) {
                throw new MySQLJdbdException("client flag error,check this.getClientFlat() method.");
            }
            PacketUtils.writeStringTerm(packetBuffer, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & ClientProtocol.CLIENT_PLUGIN_AUTH) != 0) {
            PacketUtils.writeStringTerm(packetBuffer, authPluginName.getBytes(clientCharset));
        }
        // 9. client connection attributes
        if ((clientFlag & ClientProtocol.CLIENT_CONNECT_ATTRS) != 0) {
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
        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
        return packetBuffer;
    }

    private ByteBuf createAuthenticationDataFor41(AuthenticationPlugin plugin, boolean skipPassword) {
        ByteBuf payloadBuf;
        if (skipPassword) {
            // skip password
            payloadBuf = Unpooled.EMPTY_BUFFER;
        } else {
            HandshakeV10Packet handshakeV10Packet = this.handshake;
            String seed = handshakeV10Packet.getAuthPluginDataPart1() + handshakeV10Packet.getAuthPluginDataPart2();
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
     *
     */
    private Pair<AuthenticationPlugin, Boolean> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = this.pluginMap;

        Properties<PropertyKey> properties = this.properties;
        String pluginName = this.handshake.getAuthPluginName();

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
            throw new MySQLJdbdException("AuthenticationPlugin[%s] required SSL", plugin.getClass().getName());
        }
        return new Pair<>(plugin, skipPassword);
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
        String clientVersion = ClientProtocol.class.getPackage().getImplementationVersion();
        if (clientVersion == null) {
            clientVersion = "jdbd-test";
        }
        attMap.put("_client_name", "JDBD-MySQL");
        attMap.put("_client_version", clientVersion);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_client_license", Constants.CJ_LICENSE);
        return attMap;
    }


    /**
     * @see #loadAuthenticationPluginMap()
     */
    @Nullable
    private AuthenticationPlugin loadDefaultAuthenticatePlugin() {
        String defaultPluginName = properties.getOrDefault(PropertyKey.defaultAuthenticationPlugin);
        AuthenticationPlugin plugin = null;
        try {
            if (MySQLStringUtils.hasText(defaultPluginName)) {
                plugin = loadPlugin(defaultPluginName, this);
            }
        } catch (Throwable e) {
            String message = String.format("Property[%s] value[%s] can't load plugin."
                    , PropertyKey.defaultAuthenticationPlugin.getKey()
                    , defaultPluginName);
            throw new PropertyException(e, PropertyKey.defaultAuthenticationPlugin.getKey(), message);
        }
        return plugin;
    }

    /**
     * @return a unmodifiable map
     * @see #MySQLConnectionTask(MySQLTaskAdjutant, MonoSink)
     */
    private Map<String, AuthenticationPlugin> loadAuthenticationPluginMap() {

        final String defaultPluginName = properties.getOrDefault(PropertyKey.defaultAuthenticationPlugin);

        Map<String, AuthenticationPlugin> pluginMap;

        String pluginName = null;
        byte defaultFound = 0;
        try {

            final List<String> disabledPluginList = loadDisabledPluginClassNameList();
            final Set<String> pluginClassNameSet = properties.getPropertySet(PropertyKey.authenticationPlugins);
            pluginClassNameSet.addAll(PLUGIN_NAME_LIST); // append all plugin name

            AuthenticationPlugin plugin;
            pluginMap = new HashMap<>((int) (PLUGIN_NAME_LIST.size() / 0.75F));
            for (String s : pluginClassNameSet) {
                pluginName = s;
                if (disabledPluginList.contains(pluginName)) {
                    if (pluginName.equals(defaultPluginName)) {
                        defaultFound = -1;
                    }
                    continue;
                }
                if (pluginName.equals(defaultPluginName)) {
                    defaultFound = 1;
                }
                plugin = loadPlugin(pluginName, this);
                pluginMap.put(plugin.getProtocolPluginName(), plugin);
            }

        } catch (Throwable e) {
            this.phase = Phase.LOAD_PLUGIN_ERROR;// end task
            String message = String.format("Property[%s] value[%s] can't load plugin."
                    , PropertyKey.authenticationPlugins.getKey(), pluginName);
            this.sink.error(new PropertyException(PropertyKey.authenticationPlugins.getKey(), message));
            return Collections.emptyMap();
        }

        if (defaultFound == 0) {
            this.phase = Phase.LOAD_PLUGIN_ERROR; // end task
            String message = String.format("%s[%s] not fond."
                    , PropertyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            this.sink.error(new PropertyException(PropertyKey.defaultAuthenticationPlugin.getKey(), message));
            pluginMap = Collections.emptyMap();
        } else if (defaultFound == -1) {
            this.phase = Phase.LOAD_PLUGIN_ERROR;// end task
            String message = String.format("%s[%s] disable."
                    , PropertyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            this.sink.error(new PropertyException(PropertyKey.defaultAuthenticationPlugin.getKey(), message));
            pluginMap = Collections.emptyMap();
        } else {
            pluginMap = MySQLCollections.unmodifiableMap(pluginMap);
        }
        return pluginMap;
    }

    /**
     * @return a unmodifiable list
     */
    private List<String> loadDisabledPluginClassNameList() {
        String string = properties.getProperty(PropertyKey.disabledAuthenticationPlugins);
        if (!MySQLStringUtils.hasText(string)) {
            return Collections.emptyList();
        }
        String[] mechanismArray = string.split(",");
        List<String> list = new ArrayList<>(mechanismArray.length);
        for (String mechanismOrClassName : mechanismArray) {
            String className = PLUGIN_NAME_MAP.get(mechanismOrClassName);
            if (className == null) {
                String message = String.format("Property[%s] value[%s] isn' mechanism or class name.."
                        , PropertyKey.disabledAuthenticationPlugins.getKey(), mechanismOrClassName);
                throw new PropertyException(PropertyKey.disabledAuthenticationPlugins.getKey(), message);
            }
            list.add(className);
        }
        return MySQLCollections.unmodifiableList(list);
    }


    private Charset obtainServerCharset() {
        Charset charset = CharsetMapping.getJavaCharsetByCollationIndex(this.handshake.getCollationIndex());
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        return charset;
    }


    private int createNegotiatedCapability(final HandshakeV10Packet handshake) {
        final int serverCapability = handshake.getCapabilityFlags();
        final Properties<PropertyKey> env = this.properties;

        final boolean useConnectWithDb = MySQLStringUtils.hasText(this.hostInfo.getDbName())
                && !env.getOrDefault(PropertyKey.createDatabaseIfNotExist, Boolean.class);

        return ClientProtocol.CLIENT_SECURE_CONNECTION
                | ClientProtocol.CLIENT_PLUGIN_AUTH
                | (serverCapability & ClientProtocol.CLIENT_LONG_PASSWORD)  //
                | (serverCapability & ClientProtocol.CLIENT_PROTOCOL_41)    //

                | (serverCapability & ClientProtocol.CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & ClientProtocol.CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & ClientProtocol.CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & ClientProtocol.CLIENT_LONG_FLAG)      //

                | (serverCapability & ClientProtocol.CLIENT_DEPRECATE_EOF)  //
                | (serverCapability & ClientProtocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getOrDefault(PropertyKey.useCompression, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & ClientProtocol.CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getOrDefault(PropertyKey.useAffectedRows, Boolean.class) ? 0 : (serverCapability & ClientProtocol.CLIENT_FOUND_ROWS))

                | (env.getOrDefault(PropertyKey.allowLoadLocalInfile, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_LOCAL_FILES) : 0)
                | (env.getOrDefault(PropertyKey.interactiveClient, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_INTERACTIVE) : 0)
                | (env.getOrDefault(PropertyKey.allowMultiQueries, Boolean.class) ? (serverCapability & ClientProtocol.CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getOrDefault(PropertyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverCapability & ClientProtocol.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (Constants.NONE.equals(env.getProperty(PropertyKey.connectionAttributes)) ? 0 : (serverCapability & ClientProtocol.CLIENT_CONNECT_ATTRS))
                | (env.getOrDefault(PropertyKey.sslMode, Enums.SslMode.class) != Enums.SslMode.DISABLED ? (serverCapability & ClientProtocol.CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }



    /*################################## blow private static method ##################################*/

    /**
     * @see #loadAuthenticationPluginMap()
     */
    private static AuthenticationPlugin loadPlugin(final String pluginClassName, AuthenticateAssistant assistant) {
        try {
            Class<?> pluginClass = Class.forName(pluginClassName);
            if (!AuthenticationPlugin.class.isAssignableFrom(pluginClass)) {
                throw new MySQLJdbdException("class[%s] isn't %s type."
                        , pluginClassName, AuthenticationPlugin.class.getName());
            }
            Method method = pluginClass.getMethod("getInstance", AuthenticateAssistant.class);
            if (!pluginClass.isAssignableFrom(method.getReturnType())) {
                throw new MySQLJdbdException("plugin[%s] getInstance return error type.", pluginClassName);
            }
            return (AuthenticationPlugin) method.invoke(null, assistant);
        } catch (ClassNotFoundException e) {
            throw new MySQLJdbdException(e, "plugin[%s] not found in classpath.", pluginClassName);
        } catch (NoSuchMethodException e) {
            throw new MySQLJdbdException(e, "plugin[%s] no getInstance() factory method.", pluginClassName);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new MySQLJdbdException(e, "plugin[%s] getInstance() invoke error.", pluginClassName);
        } catch (Throwable e) {
            throw new MySQLJdbdException(e, "load plugin[%s] occur error.", pluginClassName);
        }
    }


    /**
     * @return a unmodifiable map
     */
    private static Map<String, String> createPluginNameMap() {
        Map<String, String> map = new HashMap<>((int) (10 / 0.75F));

        map.put(MySQLNativePasswordPlugin.PLUGIN_NAME, MySQLNativePasswordPlugin.class.getName());
        map.put(MySQLNativePasswordPlugin.class.getName(), MySQLNativePasswordPlugin.class.getName());

        map.put(CachingSha2PasswordPlugin.PLUGIN_NAME, CachingSha2PasswordPlugin.class.getName());
        map.put(CachingSha2PasswordPlugin.class.getName(), CachingSha2PasswordPlugin.class.getName());

        map.put(MySQLClearPasswordPlugin.PLUGIN_NAME, MySQLClearPasswordPlugin.class.getName());
        map.put(MySQLClearPasswordPlugin.class.getName(), MySQLClearPasswordPlugin.class.getName());

        map.put(MySQLOldPasswordPlugin.PLUGIN_NAME, MySQLOldPasswordPlugin.class.getName());
        map.put(MySQLOldPasswordPlugin.class.getName(), MySQLOldPasswordPlugin.class.getName());

        map.put(Sha256PasswordPlugin.PLUGIN_NAME, Sha256PasswordPlugin.class.getName());
        map.put(Sha256PasswordPlugin.class.getName(), Sha256PasswordPlugin.class.getName());

        return Collections.unmodifiableMap(map);
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createPluginNameList() {
        List<String> list = new ArrayList<>(5);

        list.add(MySQLNativePasswordPlugin.class.getName());
        list.add(CachingSha2PasswordPlugin.class.getName());
        list.add(MySQLClearPasswordPlugin.class.getName());
        list.add(MySQLOldPasswordPlugin.class.getName());

        list.add(Sha256PasswordPlugin.class.getName());
        return Collections.unmodifiableList(list);
    }



    /*################################## blow private static method ##################################*/


    private enum Phase {
        RECEIVE_HANDSHAKE,
        SSL_REQUEST,
        HANDSHAKE_RESPONSE,
        AUTHENTICATE,
        LOAD_PLUGIN_ERROR,
        DISCONNECT
    }


}
