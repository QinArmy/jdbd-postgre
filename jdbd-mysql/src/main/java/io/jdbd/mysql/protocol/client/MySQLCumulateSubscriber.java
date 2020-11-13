package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
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
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private final ByteBufAllocator allocator;

    private Subscription upstream;

    //non-volatile ,modify in io.netty.channel.EventLoop .
    private ByteBuf cumulateBuffer;

    private boolean complete = false;

    private MySQLCumulateSubscriber(Connection connection) {
        //  this.connection = connection;
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
    public Mono<ByteBuf> receiveOne(Function<ByteBuf, ByteBuf> decoder) {
        Mono<ByteBuf> mono = Mono.create(sink -> {
            MonoMySQLReceiver receiver = new MonoMySQLReceiverImpl(sink, decoder);
            sink.onRequest(n -> MySQLCumulateSubscriber.this.upstream.request(128L));
            MySQLCumulateSubscriber.this.addMySQLReceiver(receiver);

        });
        return new MonoReleaseByteBuf(mono);
    }

    @Override
    public Flux<ByteBuf> receive(BiFunction<ByteBuf, Consumer<ByteBuf>, Boolean> decoder) {
        Flux<ByteBuf> flux = Flux.create(sink -> {
            FluxMonoMySQLReceiver receiver = new FluxMonoMySQLReceiver(sink, decoder);
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
        MySQLReceiver receiver = this.receiverQueue.poll();
        if (receiver != null) {
            receiver.onError(e);
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
                receiver.onError(new JdbdMySQLException("Connection close ,can't receive packet."));
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
        if (receiver == null) {
            return;
        }
        if (receiver instanceof MonoMySQLReceiver) {
            onNextForMono((MonoMySQLReceiver) receiver, this.cumulateBuffer);
        } else if (receiver instanceof FluxMySQLReceiver) {
            onNextForFlux((FluxMySQLReceiver) receiver, this.cumulateBuffer);
        } else {
            // never here
            throw new IllegalStateException("receiver unknown.");
        }
    }


    private void addMySQLReceiver(MySQLReceiver receiver) {
        if (receiver instanceof MonoMySQLReceiver || receiver instanceof FluxMySQLReceiver) {
            if (this.eventLoop.inEventLoop()) {
                doAddMySQLReceiverInEventLoop(receiver);
            } else {
                this.eventLoop.execute(() -> doAddMySQLReceiverInEventLoop(receiver));
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("unknown MySQLReceiver:%s", receiver.getClass().getName()));
        }

    }

    /**
     * @see #addMySQLReceiver(MySQLReceiver)
     */
    private void doAddMySQLReceiverInEventLoop(MySQLReceiver receiver) {
        if (this.complete) {
            receiver.onError(new JdbdMySQLException("Cannot subscribe MySQL packet because connection closed."));
        }
        final Queue<MySQLReceiver> receiverQueue = this.receiverQueue;
        if (receiverQueue.offer(receiver)) {
            if (receiver == receiverQueue.peek()) {
                drainReceiver();
            }
        } else {
            receiver.onError(new JdbdMySQLException(
                    "Cannot subscribe MySQL packet because queue limit is exceeded"));
        }
    }

    private void onNextForMono(MonoMySQLReceiver receiver, ByteBuf cumulateBuffer) {
        final ByteBuf decodedBuf = receiver.decode(cumulateBuffer);
        if (decodedBuf == null) {
            return;
        }
        if (this.receiverQueue.poll() != receiver) {
            throw new IllegalStateException("head of queue isn't  current receiver.");
        }
        receiver.success(decodedBuf);

    }

    /**
     * @see #executeOnNext(ByteBuf)
     */
    private void onNextForFlux(FluxMySQLReceiver receiver, final ByteBuf cumulateBuffer) {

        final MySQLReceiver removeReceive;
        if (receiver.decodeAndNext(cumulateBuffer)) {
            removeReceive = this.receiverQueue.poll();
            if (removeReceive != receiver) {
                throw new IllegalStateException("head of queue isn't  current receiver.");
            }
            receiver.onComplete();
        }

    }


    /*################################## blow private method ##################################*/




    /*################################## blow private static class ##################################*/

    private interface MySQLReceiver {

        void onError(Throwable t);

    }

    private interface MonoMySQLReceiver extends MySQLReceiver {

        void success(@Nullable ByteBuf byteBuf);

        /**
         * @return not null, mono complete.
         */
        @Nullable
        ByteBuf decode(ByteBuf cumulateBuffer);
    }

    private interface FluxMySQLReceiver extends MySQLReceiver {

        /**
         * @return true: flux complete.
         */
        boolean decodeAndNext(ByteBuf cumulateBuffer);


        void onComplete();
    }

    private static final class MonoMySQLReceiverImpl implements MonoMySQLReceiver {

        private final MonoSink<ByteBuf> sink;

        private final Function<ByteBuf, ByteBuf> decoderFunction;

        private MonoMySQLReceiverImpl(MonoSink<ByteBuf> sink, Function<ByteBuf, ByteBuf> decoderFunction) {
            this.sink = sink;
            this.decoderFunction = decoderFunction;
        }

        @Nullable
        @Override
        public ByteBuf decode(ByteBuf cumulateBuffer) {
            return this.decoderFunction.apply(cumulateBuffer);
        }

        @Override
        public void success(@Nullable ByteBuf byteBuf) {
            this.sink.success(byteBuf);
        }

        @Override
        public void onError(Throwable t) {
            this.sink.error(t);
        }


    }

    private static final class FluxMonoMySQLReceiver implements FluxMySQLReceiver {

        private final FluxSink<ByteBuf> sink;

        private final BiFunction<ByteBuf, Consumer<ByteBuf>, Boolean> decoder;

        private FluxMonoMySQLReceiver(FluxSink<ByteBuf> sink, BiFunction<ByteBuf, Consumer<ByteBuf>, Boolean> decoder) {
            this.sink = sink;
            this.decoder = decoder;
        }

        @Override
        public void onError(Throwable t) {
            if (!this.sink.isCancelled()) {
                this.sink.error(t);
            } else {
                LOG.warn("FluxMonoMySQLReceiver canceled,but io error.", t);
            }
        }

        @Override
        public boolean decodeAndNext(ByteBuf cumulateBuffer) {
            return this.decoder.apply(cumulateBuffer, this::onNext);
        }


        private void onNext(ByteBuf byteBuf) {
            if (!this.sink.isCancelled()) {
                this.sink.next(byteBuf);
            } else {
                LOG.debug("FluxMonoMySQLReceiver canceled,skip byteBuf.");
            }
        }

        @Override
        public void onComplete() {
            if (!this.sink.isCancelled()) {
                this.sink.complete();
            } else {
                LOG.debug("FluxMonoMySQLReceiver canceled,skip complete signal.");
            }
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
