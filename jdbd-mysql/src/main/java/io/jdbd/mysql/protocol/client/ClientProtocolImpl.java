package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

final class ClientProtocolImpl implements ClientProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolImpl.class);

    static ClientProtocolImpl getInstance(URI uri, String user, String password) {
        return new ClientProtocolImpl(uri, user, password);
    }

    private final URI uri;

    private final String user;

    private final String password;

    private final TcpClient client;

    private ClientProtocolImpl(URI uri, String user, String password) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.client = TcpClient.create()
                .host("localhost")
                .port(3306)
        ;
    }

    @Override
    public Mono<AbstractHandshakePacket> connect() {
        final AtomicInteger payloadLength = new AtomicInteger(-1);
        final AtomicInteger payloadCount = new AtomicInteger(0);
        /*return this.client
                .doOnConnected(connection -> connection.inbound().receive()
                                .bufferUntil(byteBuf -> {
                                    if (payloadLength.get() < 0) {
                                        payloadLength.set(DataTypeUtils.getInt3(byteBuf, byteBuf.readerIndex()));

                                    }
                                    return payloadCount.addAndGet(byteBuf.readableBytes()) < payloadLength.get();
                                })
                )
                .connect();*/
        return Mono.empty();
    }

    private AbstractHandshakePacket doReadHandshakePacket(ByteBuf byteBuf) {
        LOG.info("readable bytes:{}",byteBuf.readableBytes());
        short protocolVersion = DataTypeUtils.readInt1(byteBuf);
        AbstractHandshakePacket packet;
        switch (protocolVersion) {
            case 10:
                packet = readHandshakeV10(byteBuf);
                break;
            case 9:
            default:
                throw new RuntimeException();
        }
        return packet;
    }

    private HandshakeV10Packet readHandshakeV10(ByteBuf byteBuf) {
        return null;
    }

    private HandshakeV10Packet readHandshakeV9(ByteBuf byteBuf) {
        return null;
    }


}
