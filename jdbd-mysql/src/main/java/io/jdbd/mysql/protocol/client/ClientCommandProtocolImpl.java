package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ClientCommandProtocolImpl implements ClientCommandProtocol, ProtocolAssistant {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolImpl.class);


    public static Mono<ClientCommandProtocol> getInstance(MySQLUrl mySQLUrl) {
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
                // receive handshake packet from server
                .flatMap(ClientCommandProtocolImpl::handshake)
                // create ClientCommandProtocolImpl instance
                .map(pair -> new ClientCommandProtocolImpl(mySQLUrl, pair.getFirst(), pair.getSecond()))
                ;
    }

    private static Mono<Pair<Connection, HandshakeV10Packet>> handshake(Connection connection) {
        return Mono.empty();
    }


    private final MySQLUrl mySQLUrl;

    private final Connection connection;

    private final HandshakeV10Packet handshakeV10Packet;

    private final Properties properties;

    private final AtomicReference<Byte> clientCollationIndex = new AtomicReference<>(null);

    private final AtomicReference<AbstractHandshakePacket> handshakePacket = new AtomicReference<>(null);

    private final Charset clientCharset;

    private final AtomicBoolean useSsl = new AtomicBoolean(true);


    private ClientCommandProtocolImpl(MySQLUrl mySQLUrl, Connection connection, HandshakeV10Packet handshakeV10Packet) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
        this.handshakeV10Packet = handshakeV10Packet;
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();

        this.clientCharset = Charset.forName(this.properties.getRequiredProperty(PropertyKey.characterEncoding));
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
        AbstractHandshakePacket packet = this.handshakePacket.get();
        if (packet == null) {
            throw new IllegalStateException("client no handshake.");
        }
        return packet.getServerVersion();
    }

    /*################################## blow private method ##################################*/



    private Mono<Void> sendPacket(ByteBuf packetBuffer) {
        return Mono.from(this.connection.outbound().send(Mono.just(packetBuffer)));
    }


}
