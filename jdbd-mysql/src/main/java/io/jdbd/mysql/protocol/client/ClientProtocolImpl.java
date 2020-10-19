package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdRuntimeException;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class ClientProtocolImpl implements ClientProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolImpl.class);

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

    private ClientProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        this.mySQLUrl = mySQLUrl;
        this.connection = connection;
    }

    @Override
    public Mono<MySQLPacket> handshake() {
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
    public Mono<MySQLPacket> responseHandshake() {
        return Mono.empty();
    }

    @Override
    public Mono<MySQLPacket> sslRequest() {
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


}
