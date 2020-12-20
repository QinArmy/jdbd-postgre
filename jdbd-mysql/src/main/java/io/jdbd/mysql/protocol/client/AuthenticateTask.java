package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

final class AuthenticateTask extends AbstractAuthenticateTask implements AuthenticateAssistant {

    static Mono<Void> authenticate(MySQLTaskAdjutant executorAdjutant) {
        return Mono.create(sink ->
                new AuthenticateTask(executorAdjutant, sink)
                        .submit(sink::error)

        );
    }

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticateTask.class);


    private final Map<String, AuthenticationPlugin> pluginMap;

    private AuthenticationPlugin plugin;

    // non-volatile ,because all modify in netty EventLoop .
    private int authCounter = 0;

    // non-volatile ,because all modify in netty EventLoop
    private Queue<ByteBuf> pluginOutputQueue;

    private AuthenticateTask(MySQLTaskAdjutant executorAdjutant, MonoSink<Void> sink) {
        super(executorAdjutant, obtainSequenceId(executorAdjutant), sink);

        this.pluginMap = loadAuthenticationPluginMap();
    }

    private static int obtainSequenceId(MySQLTaskAdjutant executorAdjutant) {
        return (executorAdjutant.obtainNegotiatedCapability() & ClientProtocol.CLIENT_SSL) != 0
                ? 1
                : 0
                ;
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
    public HostInfo getMainHostInfo() {
        return this.hostInfo;
    }

    @Override
    public boolean isUseSsl() {
        return (this.negotiatedCapability & ClientProtocol.CLIENT_SSL) != 0;
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return this.executorAdjutant.createPacketBuffer(initialPayloadCapacity);
    }

    @Override
    public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
        return this.executorAdjutant.createPayloadBuffer(initialPayloadCapacity);
    }


    @Override
    public ServerVersion getServerVersion() {
        return this.handshakeV10Packet.getServerVersion();
    }

    @Override
    public ByteBuf moreSendPacket() {
        Queue<ByteBuf> queue = this.pluginOutputQueue;
        if (queue == null || queue.isEmpty()) {
            this.pluginOutputQueue = null;
            return null;
        }
        ByteBuf pluginOutput = queue.poll();
        if (pluginOutput == null) {
            this.pluginOutputQueue = null;
        } else {
            ByteBuf packetBuf = createPacketBuffer(pluginOutput.readableBytes());
            packetBuf.readBytes(pluginOutput);
            pluginOutput.release();
            pluginOutput = packetBuf;
            PacketUtils.writePacketHeader(pluginOutput, addAndGetSequenceId());
        }
        return pluginOutput;
    }

    /*################################## blow protected method ##################################*/

    @Override
    protected ByteBuf internalStart() {
        Pair<AuthenticationPlugin, Boolean> pair = obtainAuthenticationPlugin();
        AuthenticationPlugin plugin = pair.getFirst();
        this.plugin = plugin;
        ByteBuf pluginOut = createAuthenticationDataFor41(plugin, pair.getSecond());
        return createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut);
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer) {
        boolean taskEnd;
        taskEnd = doDecode(cumulateBuffer);
        while (!taskEnd && (this.pluginOutputQueue == null || this.pluginOutputQueue.isEmpty())
                && PacketUtils.hasOnePacket(cumulateBuffer)) {
            taskEnd = doDecode(cumulateBuffer);
        }
        return taskEnd;
    }

    private boolean doDecode(ByteBuf cumulateBuffer) {
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int payloadStartIndex = cumulateBuffer.readerIndex();
        boolean taskEnd;
        if (++this.authCounter > 100) {
            this.sink.error(new JdbdMySQLException("TooManyAuthenticationPluginNegotiations"));
            taskEnd = true;
        } else if (OkPacket.isOkPacket(cumulateBuffer)) {
            OkPacket packet = OkPacket.readPacket(cumulateBuffer, this.negotiatedCapability);
            LOG.debug("MySQL authentication success,info:{}", packet.getInfo());
            this.sink.success();
            taskEnd = true;
        } else if (ErrorPacket.isErrorPacket(cumulateBuffer)) {
            ErrorPacket error;
            Charset charset = obtainServerCharset();
            if (getSequenceId() < 2) {
                error = ErrorPacket.readPacket(cumulateBuffer, 0, charset);
            } else {
                error = ErrorPacket.readPacket(cumulateBuffer, this.negotiatedCapability, charset);
            }
            this.sink.error(MySQLExceptionUtils.createErrorPacketException(error));
            taskEnd = true;
        } else {
            taskEnd = processNextAuthenticationNegotiation(cumulateBuffer);
        }
        cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);
        return taskEnd;
    }



    /*################################## blow private method ##################################*/

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
                    this.sink.error(new JdbdMySQLException("BadAuthenticationPlugin[%s] from server.", pluginName));
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
            this.pluginOutputQueue = new ArrayDeque<>();
        }
        return false;
    }

    /**
     * @return read-only {@link ByteBuf}
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private ByteBuf createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = this.negotiatedCapability;

        final ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, ClientProtocol.MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, obtainHandshakeCollationIndex());
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        String user = this.hostInfo.getUser();
        PacketUtils.writeStringTerm(packetBuffer, user.getBytes(clientCharset));

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
            String database = this.hostInfo.getDatabase();
            if (!MySQLStringUtils.hasText(database)) {
                throw new JdbdMySQLException("client flag error,check this.getClientFlat() method.");
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
            payloadBuf = this.executorAdjutant.createPayloadBuffer(0);
        } else {
            HandshakeV10Packet handshakeV10Packet = this.handshakeV10Packet;
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


    /**
     * @see #internalStart()
     */
    private Pair<AuthenticationPlugin, Boolean> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = this.pluginMap;

        Properties properties = this.properties;
        String pluginName = this.handshakeV10Packet.getAuthPluginName();

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
        Map<String, AuthenticationPlugin> map = new HashMap<>();

        boolean defaultIsFound = false;
        for (String pluginClassName : pluginClassNameList) {
            if (disabledPluginList.contains(pluginClassName)) {
                continue;
            }
            AuthenticationPlugin plugin = loadPlugin(pluginClassName, this, this.hostInfo);
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
            JdbdMySQLException e;
            //TODO optimize
            e = new JdbdMySQLException("defaultAuthenticationPlugin[%s] not fond or disable.", defaultPluginName);
            this.sink.error(e);
            throw e;
        }
        return Collections.unmodifiableMap(map);
    }


    /*################################## blow private static method ##################################*/

    private static void appendBuildInPluginClassNameList(List<String> pluginClassNameList) {
        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());
        pluginClassNameList.add(MySQLClearPasswordPlugin.class.getName());
        pluginClassNameList.add(Sha256PasswordPlugin.class.getName());
        pluginClassNameList.add(CachingSha2PasswordPlugin.class.getName());

        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());

    }

    private static AuthenticationPlugin loadPlugin(String pluginClassName, AuthenticateAssistant assistant
            , HostInfo hostInfo) {
        try {
            Class<?> pluginClass = Class.forName(convertPluginClassName(pluginClassName));
            if (!AuthenticationPlugin.class.isAssignableFrom(pluginClass)) {
                throw new JdbdMySQLException("class[%s] isn't %s type.", AuthenticationPlugin.class.getName());
            }
            Method method = pluginClass.getMethod("getInstance", AuthenticateAssistant.class, HostInfo.class);
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

    private Charset obtainServerCharset() {
        Charset charset = CharsetMapping.getJavaCharsetByCollationIndex(this.handshakeV10Packet.getCharacterSet());
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        return charset;
    }


}
