package io.jdbd.mysql.protocol.client;

import io.jdbd.MultiResults;
import io.jdbd.PreparedStatement;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.BatchWrapper;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.StmtWrapper;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
    public ByteBuf createByteBuffer(int initialPayloadCapacity) {
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

    @Override
    public ByteBufAllocator alloc() {
        return this.taskAdjutant.alloc();
    }

    @Override
    public Server obtainServer() {
        throw new UnsupportedOperationException();
    }


    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public final Mono<ResultStates> update(String sql) {
        return ComQueryTask.update(sql, this.taskAdjutant);
    }

    @Override
    public final Flux<ResultRow> query(String sql, Consumer<ResultStates> statesConsumer) {
        return ComQueryTask.query(sql, statesConsumer, this.taskAdjutant);
    }

    @Override
    public final Flux<ResultStates> batchUpdate(List<String> sqlList) {
        return ComQueryTask.batchUpdate(sqlList, this.taskAdjutant);
    }

    @Override
    public final Mono<ResultStates> bindableUpdate(StmtWrapper wrapper) {
        return ComQueryTask.bindableUpdate(wrapper, this.taskAdjutant);
    }

    @Override
    public final Flux<ResultRow> bindableQuery(StmtWrapper wrapper) {
        return ComQueryTask.bindableQuery(wrapper, this.taskAdjutant);
    }

    @Override
    public final Flux<ResultStates> bindableBatch(BatchWrapper wrapper) {
        return ComQueryTask.bindableBatch(wrapper, this.taskAdjutant);
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql) {
        return ComPreparedTask.prepare(sql, this.taskAdjutant);
    }

    @Override
    public final MultiResults multiStmt(List<String> commandList) {
        return ComQueryTask.multiStmt(commandList, this.taskAdjutant);
    }

    @Override
    public final MultiResults multiBindable(List<StmtWrapper> wrapperList) {
        return ComQueryTask.bindableMultiStmt(wrapperList, this.taskAdjutant);
    }

    @Override
    public Mono<Void> closeGracefully() {
        return QuitTask.quit(this.taskAdjutant);
    }


    /*################################## blow private method ##################################*/


}
