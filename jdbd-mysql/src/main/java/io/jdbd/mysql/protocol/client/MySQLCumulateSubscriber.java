package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.netty.ByteBufFlux;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.function.BiFunction;

/**
 * This class is a implementation of {@link MySQLCumulateReceiver}.
 *
 * @see NettyInbound#receive()
 * @see ByteBufFlux#retain()
 */
final class MySQLCumulateSubscriber implements CoreSubscriber<ByteBuf>, MySQLCumulateReceiver {

    static MySQLCumulateReceiver from(Connection connection) {
        MySQLCumulateSubscriber receiver = new MySQLCumulateSubscriber(connection);
        connection.inbound().receive()
                .retain() // for below cumulate
                .subscribe(receiver);
        return receiver;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCumulateSubscriber.class);


    private final Queue<MySQLReceiver> receiverQueue = Queues.<MySQLReceiver>small().get();

    private final EventLoop eventLoop;

    private final Connection connection;

    private final ByteBufAllocator allocator;

    private Subscription upstream;

    //non-volatile ,modify in io.netty.channel.EventLoop .
    private ByteBuf cumulateBuffer;

    private boolean complete = false;

    private MySQLCumulateSubscriber(Connection connection) {
        this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();
        this.allocator = connection.channel().alloc();

    }

    /*################################## blow CoreSubscriber method ##################################*/

    /**
     * @see NettyInbound#receive()
     */
    @Override
    public void onSubscribe(Subscription s) {
        this.upstream = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf byteBufFromPeer) {
        if (byteBufFromPeer == this.cumulateBuffer) {
            // due to byteBufFromPeer.release()
            throw new JdbdMySQLException("previous cumulateBuffer handle error.");
        }
        if (this.eventLoop.inEventLoop()) {
            executeOnNext(byteBufFromPeer);
        } else {
            this.eventLoop.execute(() -> executeOnNext(byteBufFromPeer));
        }
    }

    @Override
    public void onError(Throwable e) {
        if (this.eventLoop.inEventLoop()) {
            executeOnError(e);
        } else {
            this.eventLoop.execute(() -> executeOnError(e));
        }
    }

    @Override
    public void onComplete() {
        if (this.eventLoop.inEventLoop()) {
            executeOnComplete();
        } else {
            this.eventLoop.execute(this::executeOnComplete);
        }
    }

    /*################################## blow MySQLCumulateReceiver method ##################################*/

    @Override
    public Mono<ByteBuf> receiveOnePacket() {
        return receiveOne(PacketDecoders::packetDecoder);
    }

    @Override
    public Mono<ByteBuf> receiveOne(BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> decoder) {
        Mono<ByteBuf> mono = Mono.create(sink -> {
            MySQLReceiver receiver = new MonoMySQLReceiverImpl(sink, decoder);
            sink.onRequest(n -> MySQLCumulateSubscriber.this.upstream.request(128L));
            MySQLCumulateSubscriber.this.addMySQLReceiver(receiver);

        });
        return new MonoReleaseByteBuf(mono);
    }

    @Override
    public Flux<ByteBuf> receive(BiFunction<ByteBuf, FluxSink<ByteBuf>, Boolean> decoder) {
        Flux<ByteBuf> flux = Flux.create(sink -> {
            MySQLReceiver receiver = new FluxMonoMySQLReceiver(sink, decoder);
            sink.onRequest(n -> MySQLCumulateSubscriber.this.upstream.request(512L));
            MySQLCumulateSubscriber.this.addMySQLReceiver(receiver);
        });
        return new FluxReleaseByteBuf(flux);
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #onNext(ByteBuf)
     */
    private void executeOnNext(ByteBuf byteBufFromPeer) {

        //  cumulate Buffer
        ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            cumulateBuffer = byteBufFromPeer;
        } else {
            if (cumulateBuffer == byteBufFromPeer) {
                // due to byteBufFromPeer.release()
                throw new JdbdMySQLException("byteBufFromPeer duplication");
            }
            cumulateBuffer = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                    this.allocator, cumulateBuffer, byteBufFromPeer);
        }
        this.cumulateBuffer = cumulateBuffer;
        drainReceiver();
    }

    private void executeOnError(Throwable e) {
        if (MySQLExceptionUtils.isContainFatalIoException(e)) {
            MySQLReceiver receiver;
            while ((receiver = this.receiverQueue.poll()) != null) {
                receiver.error(e);
            }
        } else {
            MySQLReceiver receiver = this.receiverQueue.poll();
            if (receiver != null) {
                receiver.error(e);
            }
        }

    }

    private void executeOnComplete() {
        if (this.complete) {
            return;
        }
        ByteBuf cumulateBuf = this.cumulateBuffer;
        if (cumulateBuf != null) {
            cumulateBuf.release();
            this.cumulateBuffer = null;
        }
        Queue<MySQLReceiver> receiverQueue = this.receiverQueue;
        MySQLReceiver receiver;
        while ((receiver = receiverQueue.poll()) != null) {
            try {
                receiver.error(new JdbdMySQLException("Connection close ,can't receive packet."));
            } catch (Throwable e) {
                LOG.warn("downstream handler error event occur error.", e);
            }
        }
        this.complete = true;
    }

    /**
     * @see #executeOnNext(ByteBuf)
     * @see #doAddMySQLReceiverInEventLoop(MySQLReceiver)
     */
    private void drainReceiver() {

        if (this.cumulateBuffer == null) {
            return;
        }
        // 1. invoke receiver
        final MySQLReceiver receiver = this.receiverQueue.peek();
        if (receiver != null) {
            boolean decodeEnd;
            try {
                decodeEnd = receiver.decodeAndNext(this.cumulateBuffer);
            } catch (Throwable e) {
                if (MySQLExceptionUtils.isContainFatalIoException(e)) {
                    // fatal io error, must close connection.
                    this.connection.channel().close();
                }
                throw e;

            }
            if (decodeEnd) {
                this.receiverQueue.poll();
            }
        }
    }


    private void addMySQLReceiver(MySQLReceiver receiver) {
        if (this.eventLoop.inEventLoop()) {
            doAddMySQLReceiverInEventLoop(receiver);
        } else {
            this.eventLoop.execute(() -> doAddMySQLReceiverInEventLoop(receiver));
        }

    }

    /**
     * @see #addMySQLReceiver(MySQLReceiver)
     */
    private void doAddMySQLReceiverInEventLoop(MySQLReceiver receiver) {
        if (this.complete) {
            receiver.error(new JdbdMySQLException("Cannot subscribe MySQL packet because connection closed."));
        }
        final Queue<MySQLReceiver> receiverQueue = this.receiverQueue;
        if (receiverQueue.offer(receiver)) {
            if (receiver == receiverQueue.peek()) {
                drainReceiver();
            }
        } else {
            receiver.error(new JdbdMySQLException(
                    "Cannot subscribe MySQL packet because queue limit is exceeded"));
        }
    }



    /*################################## blow private method ##################################*/




    /*################################## blow private static class ##################################*/

    private interface MySQLReceiver {

        void error(Throwable t);

        /**
         * @return true: flux complete.
         */
        boolean decodeAndNext(ByteBuf cumulateBuffer);

    }

    private static final class MonoMySQLReceiverImpl implements MySQLReceiver {

        private final MonoSink<ByteBuf> sink;

        private final BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> decoderFunction;

        private MonoMySQLReceiverImpl(MonoSink<ByteBuf> sink, BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> decoderFunction) {
            this.sink = sink;
            this.decoderFunction = decoderFunction;
        }

        @Override
        public void error(Throwable t) {
            this.sink.error(t);
        }

        @Override
        public boolean decodeAndNext(ByteBuf cumulateBuffer) {
            return this.decoderFunction.apply(cumulateBuffer, this.sink);
        }
    }

    private static final class FluxMonoMySQLReceiver implements MySQLReceiver {

        private final FluxSink<ByteBuf> sink;

        private final BiFunction<ByteBuf, FluxSink<ByteBuf>, Boolean> decoder;

        private FluxMonoMySQLReceiver(FluxSink<ByteBuf> sink, BiFunction<ByteBuf, FluxSink<ByteBuf>, Boolean> decoder) {
            this.sink = sink;
            this.decoder = decoder;
        }

        @Override
        public void error(Throwable t) {
            if (!this.sink.isCancelled()) {
                this.sink.error(t);
            } else {
                LOG.warn("FluxMonoMySQLReceiver canceled,but io error.", t);
            }
        }

        @Override
        public boolean decodeAndNext(ByteBuf cumulateBuffer) {
            return this.decoder.apply(cumulateBuffer, this.sink);
        }

    }


    private static final class FluxReleaseByteBuf extends FluxOperator<ByteBuf, ByteBuf> {

        private FluxReleaseByteBuf(Flux<? extends ByteBuf> source) {
            super(source);
        }

        @Override
        public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
            this.source.subscribe(new ReleaseByteBufCoreSubscriber(actual));
        }
    }

    private static final class MonoReleaseByteBuf extends MonoOperator<ByteBuf, ByteBuf> {

        private MonoReleaseByteBuf(Mono<? extends ByteBuf> source) {
            super(source);
        }

        @Override
        public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
            this.source.subscribe(new ReleaseByteBufCoreSubscriber(actual));
        }
    }


    private static final class ReleaseByteBufCoreSubscriber implements CoreSubscriber<ByteBuf> {

        private final CoreSubscriber<? super ByteBuf> actual;

        private ReleaseByteBufCoreSubscriber(CoreSubscriber<? super ByteBuf> actual) {
            this.actual = actual;
            Operators.enableOnDiscard(actual.currentContext(), ReferenceCountUtil::release);
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.actual.onSubscribe(s);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            // this.actual.onNext(byteBuf);
            try {
                this.actual.onNext(byteBuf);
            } finally {
                try {
                    // LOG.debug("byteBuf reference count:{},{}@{}",byteBuf.refCnt(),byteBuf.getClass().getName(),System.identityHashCode(byteBuf));
                    byteBuf.release();
                } catch (Throwable e) {
                    this.actual.onError(e);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            this.actual.onError(t);
        }

        @Override
        public void onComplete() {
            this.actual.onComplete();
        }
    }


}
