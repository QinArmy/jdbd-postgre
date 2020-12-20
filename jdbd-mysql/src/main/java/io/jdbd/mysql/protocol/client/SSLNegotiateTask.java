package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

final class SSLNegotiateTask extends AbstractAuthenticateTask {

    static Mono<Void> sslNegotiate(MySQLTaskAdjutant executorAdjutant) {
        return Mono.create(sink ->
                new SSLNegotiateTask(executorAdjutant, sink)
                        .submit(sink::error)

        );
    }


    private SSLNegotiateTask(MySQLTaskAdjutant executorAdjutant, MonoSink<Void> sink) {
        super(executorAdjutant, 0, sink);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html">Protocol::SSLRequest</a>
     */
    @Override
    protected ByteBuf internalStart() {

        ByteBuf packetBuf = this.executorAdjutant.createPacketBuffer(32);
        // 1. client_flag
        PacketUtils.writeInt4(packetBuf, this.negotiatedCapability);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuf, ClientProtocol.MAX_PACKET_SIZE);
        // 3. character_set,
        PacketUtils.writeInt1(packetBuf, obtainHandshakeCollationIndex());
        // 4. filler
        packetBuf.writeZero(23);

        PacketUtils.writePacketHeader(packetBuf, addAndGetSequenceId());
        return packetBuf;
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer) {
        return true;
    }


    private Mono<Void> performSslHandshake() {

//        Channel channel = this.connection.channel();
//        final SslHandler sslHandler = createSslHandler(channel);
//
//        ChannelPipeline pipeline = channel.pipeline();
//
//        if (pipeline.get(NettyPipeline.ProxyHandler) != null) {
//            pipeline.addAfter(NettyPipeline.ProxyHandler, NettyPipeline.SslHandler, sslHandler);
//        } else if (pipeline.get(NettyPipeline.ProxyProtocolReader) != null) {
//            pipeline.addAfter(NettyPipeline.ProxyProtocolReader, NettyPipeline.SslHandler, sslHandler);
//        } else {
//            pipeline.addFirst(NettyPipeline.SslHandler, sslHandler);
//        }
        return Mono.empty();
    }

}
