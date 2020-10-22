package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.StringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final AtomicBoolean useSsl = new AtomicBoolean(true);


    private ClientProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();
        this.clientCharset = Charset.forName(this.properties.getRequiredProperty(PropertyKey.characterEncoding));

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
        Mono<ByteBuf> mono;
        if (packet instanceof HandshakeV10Packet) {
            HandshakeV10Packet handshakeV10Packet = (HandshakeV10Packet) packet;
            if ((handshakeV10Packet.getCapabilityFlags() & ClientProtocol.CLIENT_PROTOCOL_41) != 0) {
                mono = createHandshakeResponse41();
            } else {
                mono = createHandshakeResponse320();
            }
        } else {
            mono = createHandshakeResponse320();
        }
        return mono.flatMap(this::sendPacket)
                .then(Mono.defer(this::readAuthResponse))
                ;
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
        return this.useSsl.get();
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return PacketUtils.createPacketBuffer(this.connection, initialPayloadCapacity);
    }

    @Override
    public ByteBuf createOneSizePacketForWrite(int payloadByte) {
        return PacketUtils.createOneSizePacket(this.connection, payloadByte);
    }

    @Override
    public ByteBuf createEmptyPacketForWrite() {
        return PacketUtils.createEmptyPacket(this.connection);
    }

    @Override
    public ServerVersion getServerVersion() {
        AbstractHandshakePacket packet = this.handshakePacket.get();
        if (packet == null) {
            throw new IllegalStateException("client no handshake.");
        }
        return packet.getServerVersion();
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

    private Mono<ByteBuf> createHandshakeResponse320() {
        return Mono.empty();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private Mono<ByteBuf> createHandshakeResponse41() {
        final Charset clientCharset = this.clientCharset;
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
        PacketUtils.writeStringTerm(packetBuffer, user.getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        Pair<ByteBuf, String> authenticatePair = createAuthenticationPair();
        if ((clientFlag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            PacketUtils.writeStringLenEnc(packetBuffer, authenticatePair.getFirst());
        } else {
            ByteBuf authBuffer = authenticatePair.getFirst();
            PacketUtils.writeInt1(packetBuffer, authBuffer.readableBytes());
            packetBuffer.writeBytes(authBuffer);
        }

        // 7. database
        if ((clientFlag & CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.mySQLUrl.getOriginalDatabase();
            if (!StringUtils.hasText(database)) {
                throw new JdbdMySQLException("client flag error,check this.getClientFlat() method.");
            }
            PacketUtils.writeStringTerm(packetBuffer, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & CLIENT_PLUGIN_AUTH) != 0) {
            PacketUtils.writeStringTerm(packetBuffer, authenticatePair.getSecond().getBytes(clientCharset));
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

        return Mono.just(packetBuffer);
    }

    private Mono<Void> sendPacket(ByteBuf packetBuffer) {
        return Mono.empty();
    }

    private Mono<MySQLPacket> readAuthResponse() {
        return Mono.empty();
    }

    private int getClientFlat() {
        HandshakeV10Packet handshakeV10Packet = (HandshakeV10Packet) this.handshakePacket.get();
        final int serverFlag = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = StringUtils.hasText(this.mySQLUrl.getOriginalDatabase())
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

    private Pair<ByteBuf, String> createAuthenticationPair() {
        return null;
    }

    private AuthenticationPlugin obtainAuthenticationPlugin() {
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
        return plugin;
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

    private HandshakeV10Packet obtainHandshakeV10Packet() {
        AbstractHandshakePacket packet = obtainHandshakePacket();
        if (!(packet instanceof HandshakeV10Packet)) {
            throw new IllegalStateException(
                    String.format("handshakePacket[%s] isn't HandshakeV10Packet.", packet.getClass().getName()));
        }
        return (HandshakeV10Packet) packet;
    }

    private AbstractHandshakePacket obtainHandshakePacket() {
        AbstractHandshakePacket handshakePacket = this.handshakePacket.get();
        if (handshakePacket == null) {
            throw new IllegalStateException("client no handshake.");
        }
        return handshakePacket;
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

    /*################################## blow static method ##################################*/

    public static AuthenticationPlugin loadPlugin(String pluginClassName, ProtocolAssistant assistant
            , HostInfo hostInfo) {
        try {
            Class<?> pluginClass = Class.forName(pluginClassName);
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

}
