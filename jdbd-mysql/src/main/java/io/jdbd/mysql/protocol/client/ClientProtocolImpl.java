package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.StringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.jdbd.mysql.protocol.conf.PropertyDefinitions.SslMode;

public final class ClientProtocolImpl implements ClientProtocol {

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

    private final Environment env;

    private final AtomicReference<AbstractHandshakePacket> handshakePacket = new AtomicReference<>(null);

    private ClientProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.env = this.mySQLUrl.getHosts().get(0).getEnvironment();
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
                        payloadLength.set(DataTypeUtils.getInt3(byteBuf, byteBuf.readerIndex()));
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

    private MySQLPacket parseHandshakePacket(ByteBuf byteBuf) {

        if (ErrorPacket.isErrorPacket(byteBuf)) {
            return ErrorPacket.readPacket(byteBuf);
        }
        MySQLPacket packet;
        short version = DataTypeUtils.getInt1(byteBuf, MySQLPacket.HEAD_LENGTH);
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
            mono = Mono.just(packet);
        } else if (packet instanceof HandshakeV9Packet) {
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

    private Mono<MySQLPacket> writeHandshakeResponse41() {
        int clientFlag = getClientFlat();
//        this.connection.outbound()
//                .sendByteArray()
        return Mono.empty();
    }

    private int getClientFlat() {
        HandshakeV10Packet handshakeV10Packet = (HandshakeV10Packet) this.handshakePacket.get();
        final int serverFlag = handshakeV10Packet.getCapabilityFlags();
        final Environment env = this.env;

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
                | (env.getProperty(PropertyKey.useCompression.getKeyName(), Boolean.class, Boolean.FALSE) ? (serverFlag & CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverFlag & CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getProperty(PropertyKey.useAffectedRows.getKeyName(), Boolean.class, Boolean.FALSE) ? 0 : (serverFlag & CLIENT_FOUND_ROWS))

                | (env.getProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), Boolean.class, Boolean.FALSE) ? (serverFlag & CLIENT_LOCAL_FILES) : 0)
                | (env.getProperty(PropertyKey.interactiveClient.getKeyName(), Boolean.class, Boolean.FALSE) ? (serverFlag & CLIENT_INTERACTIVE) : 0)
                | (env.getProperty(PropertyKey.allowMultiQueries.getKeyName(), Boolean.class, Boolean.FALSE) ? (serverFlag & CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getProperty(PropertyKey.disconnectOnExpiredPasswords.getKeyName(), Boolean.class, Boolean.TRUE) ? 0 : (serverFlag & CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (NONE.equals(env.getProperty(PropertyKey.connectionAttributes.getKeyName())) ? 0 : (serverFlag & CLIENT_CONNECT_ATTRS))
                | (env.getProperty(PropertyKey.sslMode.getKeyName(), SslMode.class, SslMode.PREFERRED) != SslMode.DISABLED ? (serverFlag & CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }


}
