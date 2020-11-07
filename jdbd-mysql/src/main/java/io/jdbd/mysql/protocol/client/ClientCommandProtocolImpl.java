package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.EofPacket;
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

    private final MySQLCumulateReceiver cumulateReceiver;

    private final HandshakeV10Packet handshakeV10Packet;

    private final byte clientCollationIndex;

    private final Charset clientCharset;

    private final int negotiatedCapability;


    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl connectionProtocol) {
        this.mySQLUrl = connectionProtocol.getMySQLUrl();
        this.connection = connectionProtocol.getConnection();
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();
        this.cumulateReceiver = connectionProtocol.getCumulateReceiver();

        this.handshakeV10Packet = connectionProtocol.getHandshakeV10Packet();
        this.clientCollationIndex = connectionProtocol.getClientCollationIndex();
        this.clientCharset = connectionProtocol.getHandshakeCharset();
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
                .then(Mono.defer(this::receiveComQueryResponseMeta))
                .map(payloadBuf -> handleComQueryResponseMeta(payloadBuf, sequenceId))
                .then(Mono.empty())
                ;

    }

    @Override
    public long getId() {
        return this.handshakeV10Packet.getThreadId();
    }

    /*################################## blow private method ##################################*/

    private MySQLRowMeta handleComQueryResponseMeta(final ByteBuf metaBuf, AtomicInteger sequenceId) {
        readPacketHeader(metaBuf, sequenceId);
        final int negotiatedCapability = this.negotiatedCapability;
        // 1. metadata_follows
        final byte metadataFollows;
        if ((negotiatedCapability & CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            // 0: RESULTSET_METADATA_NONE , 1:RESULTSET_METADATA_FULL
            metadataFollows = metaBuf.readByte();
        } else {
            metadataFollows = 1;
        }
        // 2. column_count
        final int columnCount = (int) PacketUtils.readLenEnc(metaBuf);
        final MySQLRowMeta rowMeta;
        if ((negotiatedCapability & CLIENT_OPTIONAL_RESULTSET_METADATA) == 0 || metadataFollows == 1) {
            // 3. Field metadata ,read row meta.
            rowMeta = readRowMeta(metaBuf, columnCount, sequenceId);
        } else {
            throw new JdbdMySQLException("COM_QUERY response packet,not present field metadata.");
        }
        if ((negotiatedCapability & CLIENT_DEPRECATE_EOF) == 0) {
            // 4. read EOF packet Marker to set the end of metadata
            readPacketHeader(metaBuf, sequenceId);
            EofPacket.isEofPacket(metaBuf);
        }
        return rowMeta;
    }

    private MySQLRowMeta readRowMeta(final ByteBuf metaBuf, final int columnCount, AtomicInteger sequenceId) {
        int prevSequenceId = sequenceId.get();
        ColumnMeta[] columnMetas = new ColumnMeta[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int startReaderIndex = metaBuf.readerIndex();
            int packetLength = PacketUtils.readPacketLength(metaBuf);
            int currentSequenceId = PacketUtils.readInt1(metaBuf);

            if (currentSequenceId != prevSequenceId + 1) {
                LOG.error("COM_QUERY response Field metadata exception,expected[{}] but[{}] ."
                        , prevSequenceId + 1, currentSequenceId);
            }
            prevSequenceId = currentSequenceId;

            columnMetas[i] = readColumnMeta(metaBuf);
            if (metaBuf.readerIndex() != startReaderIndex + packetLength) {
                throw new JdbdMySQLException(String.format(
                        "Field metadata read error,currentSequenceId[%s]", currentSequenceId));
            }
        }
        return createRowMeta(columnMetas);
    }

    private ColumnMeta readColumnMeta(final ByteBuf metaBuf) {
        final Charset charset = this.clientCharset;

        // 1. catalog
        PacketUtils.readStringLenEnc(metaBuf, charset);
        // 2. schema
        PacketUtils.readStringLenEnc(metaBuf, charset);
        // 3. table
        PacketUtils.readStringLenEnc(metaBuf, charset);
        // 4. org_table
        PacketUtils.readStringLenEnc(metaBuf, charset);

        // 5. name ,  alias
        final String alias = PacketUtils.readStringLenEnc(metaBuf, charset);
        // 6. org_name
        PacketUtils.readStringLenEnc(metaBuf, charset);


        return null;
    }

    private MySQLRowMeta createRowMeta(ColumnMeta[] columnMetas) {
        return MySQLRowMeta.from(columnMetas);
    }


    /**
     * packet header have read.
     *
     * @see #readPacketHeader(ByteBuf, AtomicInteger)
     * @see #sendCommandPacket(ByteBuf, AtomicInteger sequenceId)
     */
    private Mono<ByteBuf> receivePayload(AtomicInteger sequenceId) {
        return this.cumulateReceiver.receiveOnePacket()
                .doOnNext(packetBuf -> readPacketHeader(packetBuf, sequenceId))
                ;
    }

    private Mono<ByteBuf> receiveComQueryResponseMeta() {
        return this.cumulateReceiver.receiveOne(
                cumulateBuf -> PacketDecoders.comQueryResponseDecoder(cumulateBuf, this.negotiatedCapability));
    }

    /**
     * @see #receivePayload(AtomicInteger)
     * @see #sendCommandPacket(ByteBuf, AtomicInteger sequenceId)
     */
    private void readPacketHeader(ByteBuf packetBuf, AtomicInteger sequenceId) {
        packetBuf.skipBytes(3);
        final int sequenceIdFromServer = PacketUtils.readInt1(packetBuf);
        if (!sequenceId.compareAndSet(sequenceIdFromServer - 1, sequenceIdFromServer)) {
            throw new JdbdMySQLException(
                    "sequenceId[%s] form server error,should be %s .", sequenceIdFromServer, sequenceIdFromServer - 1);
        }
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
