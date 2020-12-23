package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

public final class ClientCommandProtocolImpl extends AbstractClientProtocol implements ClientCommandProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolImpl.class);


    public static Mono<ClientCommandProtocol> create(HostInfo hostInfo, EventLoopGroup eventLoopGroup) {
        return ClientConnectionProtocolImpl.create(hostInfo, eventLoopGroup)
                .map(cp -> {
                    ClientCommandProtocolImpl dp = new ClientCommandProtocolImpl(cp);
                    DefaultCommTaskExecutor.updateProtocolAdjutant(cp.commTaskExecutor, dp);
                    return dp;
                });

    }


    private final HandshakeV10Packet handshakeV10Packet;

    private final int negotiatedCapability;

    private final Charset charsetClient;

    private final Charset charsetResults;

    private final int maxBytesPerCharClient;

    private final Map<Integer, CharsetMapping.CustomCollation> customCollationMap;

    private final ZoneOffset zoneOffsetClient;

    private final ZoneOffset zoneOffsetDatabase;

    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl cp) {
        super(cp.obtainHostInfo(), cp.taskAdjutant);

        this.handshakeV10Packet = cp.obtainHandshakeV10Packet();
        this.negotiatedCapability = cp.obtainNegotiatedCapability();
        this.charsetClient = cp.obtainCharsetClient();
        this.charsetResults = cp.obtainCharsetResults();

        this.maxBytesPerCharClient = cp.obtainMaxBytesPerCharClient();
        this.customCollationMap = cp.obtainCustomCollationMap();
        this.zoneOffsetClient = cp.obtainZoneOffsetClient();
        this.zoneOffsetDatabase = cp.obtainZoneOffsetDatabase();
    }

    @Override
    public Mono<Void> closeGracefully() {
        return QuitTask.quit(this.taskAdjutant);
    }

    /*################################## blow ClientProtocolAdjutant method ##################################*/


    @Override
    public long getId() {
        return this.handshakeV10Packet.getThreadId();
    }

    @Override
    public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return this.taskAdjutant.createPacketBuffer(initialPayloadCapacity);
    }

    @Override
    public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
        return this.taskAdjutant.createPacketBuffer(initialPayloadCapacity);
    }

    @Override
    public int obtainMaxBytesPerCharClient() {
        return this.maxBytesPerCharClient;
    }

    @Override
    public Charset obtainCharsetClient() {
        return this.charsetClient;
    }

    @Override
    public Charset obtainCharsetResults() {
        return this.charsetResults;
    }

    @Override
    public int obtainNegotiatedCapability() {
        return this.negotiatedCapability;
    }

    @Override
    public Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
        return this.customCollationMap;
    }

    @Override
    public ZoneOffset obtainZoneOffsetDatabase() {
        return this.zoneOffsetDatabase;
    }

    @Override
    public ZoneOffset obtainZoneOffsetClient() {
        return this.zoneOffsetClient;
    }

    @Override
    public HandshakeV10Packet obtainHandshakeV10Packet() {
        return this.handshakeV10Packet;
    }

    /*################################## blow private method ##################################*/


}
