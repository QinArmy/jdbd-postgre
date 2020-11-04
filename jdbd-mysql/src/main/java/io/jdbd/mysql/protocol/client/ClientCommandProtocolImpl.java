package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public final class ClientCommandProtocolImpl implements ClientCommandProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolImpl.class);


    public static Mono<ClientCommandProtocol> getInstance(ClientConnectionProtocolImpl connectionProtocol) {
        return Mono.just(new ClientCommandProtocolImpl(connectionProtocol));
    }


    private final MySQLUrl mySQLUrl;

    private final Properties properties;

    private final Connection connection;

    private final MySQLPacketSubscriber<ByteBuf> packetReceiver;

    private final HandshakeV10Packet handshakeV10Packet;

    private final byte clientCollationIndex;

    private final Charset clientCharset;

    private final int negotiatedCapability;


    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl connectionProtocol) {
        this.mySQLUrl = connectionProtocol.getMySQLUrl();
        this.connection = connectionProtocol.getConnection();
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();
        this.packetReceiver = connectionProtocol.getPacketReceiver();

        this.handshakeV10Packet = connectionProtocol.getHandshakeV10Packet();
        this.clientCollationIndex = connectionProtocol.getClientCollationIndex();
        this.clientCharset = connectionProtocol.getClientCharset();
        this.negotiatedCapability = connectionProtocol.getNegotiatedCapability();
    }

    /*################################## blow ClientCommandProtocol method ##################################*/

    @Override
    public Mono<MySQLPacket> comQueryForResultSet(String sql) {
        final ByteBuf packetBuf = PacketUtils.createPacketBuffer(this.connection, sql.length() + 1);

        packetBuf.writeByte(ClientConstants.COM_QUERY) //write command header
                .writeBytes(sql.getBytes(this.clientCharset)) //write query
        ;
        final AtomicInteger sequenceId = new AtomicInteger(-1);

        return sendCommandPacket(packetBuf, sequenceId)
                .then(Mono.defer(() -> receivePayload(sequenceId)))
                .flatMap(this::handleComQueryResponse)
                ;

    }

    /*################################## blow private method ##################################*/

    private Mono<MySQLPacket> handleComQueryResponse(ByteBuf payloadBuf) {
        LOG.info("payloadBuf readableBytes:{}", payloadBuf.readableBytes());
        return Mono.empty();
    }


    /**
     * packet header have read.
     *
     * @see #readPacketHeader(ByteBuf, AtomicInteger)
     * @see #sendCommandPacket(ByteBuf, AtomicInteger sequenceId)
     */
    private Mono<ByteBuf> receivePayload(AtomicInteger sequenceId) {
        return this.packetReceiver.receiveOne()
                .flatMap(packetBuf -> readPacketHeader(packetBuf, sequenceId));
    }

    /**
     * @see #receivePayload(AtomicInteger)
     * @see #sendCommandPacket(ByteBuf, AtomicInteger sequenceId)
     */
    private Mono<ByteBuf> readPacketHeader(ByteBuf packetBuf, AtomicInteger sequenceId) {
        packetBuf.skipBytes(3);
        final int sequenceIdFromServer = PacketUtils.readInt1(packetBuf);
        Mono<ByteBuf> mono;
        if (sequenceId.compareAndSet(sequenceIdFromServer - 1, sequenceIdFromServer)) {
            mono = Mono.just(packetBuf);
        } else {
            mono = Mono.error(new JdbdMySQLException(
                    "sequenceId[%s] form server error,should be %s .", sequenceIdFromServer, sequenceIdFromServer - 1));
        }
        return mono;
    }

    /**
     * @see #receivePayload(AtomicInteger)
     * @see #readPacketHeader(ByteBuf, AtomicInteger)
     */
    private Mono<Void> sendCommandPacket(ByteBuf packetBuffer, AtomicInteger sequenceId) {
        PacketUtils.writePacketHeader(packetBuffer, sequenceId.addAndGet(1));
        return Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuffer)));
    }


}
