package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.ByteToMessageDecoder;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @see NettyInbound#receive()
 * @see ByteBufFlux#retain()
 */
final class MySQLCumulateSubscriber implements CoreSubscriber<ByteBuf>, MySQLCumulateReceiver {

    static MySQLCumulateReceiver from(Connection connection) {
        MySQLCumulateSubscriber receiver = new MySQLCumulateSubscriber(connection);
        connection.inbound().receive()
                .retain()
                .subscribe(receiver);
        connection.channel().eventLoop();
        return receiver;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCumulateSubscriber.class);

    private static final AtomicIntegerFieldUpdater<MySQLCumulateSubscriber> COMPLETE = AtomicIntegerFieldUpdater
            .newUpdater(MySQLCumulateSubscriber.class, "complete");

    private final Queue<MySQLReceiver> receiverQueue = Queues.<MySQLReceiver>small().get();

    private final EventLoop eventLoop;

    private Subscription upstream;

    //non-volatile ,modify in io.netty.channel.EventLoop .
    private ByteBuf cumulateBuffer;


    private volatile int complete = 0;

    private MySQLCumulateSubscriber(Connection connection) {
        //  this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();

    }

    /*################################## blow CoreSubscriber method ##################################*/

    /**
     * @see NettyInbound#receive()
     */
    @Override
    public void onSubscribe(Subscription s) {
        this.upstream = s;
        // s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf byteBufFromPeer) {
        if (this.eventLoop.inEventLoop()) {
            executeOnNext(byteBufFromPeer);
        } else {
            this.eventLoop.execute(() -> executeOnNext(byteBufFromPeer));
        }
    }

    @Override
    public void onError(Throwable t) {
        MySQLReceiver receiver = this.receiverQueue.poll();
        if (receiver != null) {
            receiver.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (COMPLETE.compareAndSet(this, 0, 1)) {
            Queue<MySQLReceiver> receiverQueue = this.receiverQueue;
            MySQLReceiver receiver;
            while ((receiver = receiverQueue.poll()) != null) {
                if (receiver instanceof MonoMySQLReceiver) {
                    ((MonoMySQLReceiver) receiver).success(null);
                } else if (receiver instanceof FluxMySQLReceiver) {
                    ((FluxMySQLReceiver) receiver).onComplete();
                }
            }
        }
    }

    /*################################## blow MySQLCumulateReceiver method ##################################*/

    @Override
    public Mono<ByteBuf> receiveOnePacket() {
        return receiveOne(MySQLCumulateSubscriber::parseMySQLPacket);
    }

    @Override
    public Mono<ByteBuf> receiveOne(Function<ByteBuf, ByteBuf> decoder) {
        Mono<ByteBuf> mono = Mono.create(sink -> {
            MonoMySQLReceiver receiver = new MonoMySQLReceiverImpl(sink, decoder);
            sink.onRequest(n -> MySQLCumulateSubscriber.this.upstream.request(1L));
            MySQLCumulateSubscriber.this.addMySQLReceiver(receiver);

        });
        return new MonoReleaseByteBuf(mono);
    }

    @Override
    public Flux<ByteBuf> receive(BiFunction<ByteBuf, List<ByteBuf>, Boolean> decoder) {
        Flux<ByteBuf> flux = Flux.create(sink -> {
            FluxMonoMySQLReceiver receiver = new FluxMonoMySQLReceiver(sink, decoder);
            sink.onRequest(n -> MySQLCumulateSubscriber.this.upstream.request(1L));
            MySQLCumulateSubscriber.this.addMySQLReceiver(receiver);
        });
        return new FluxReleaseByteBuf(flux);
    }


    /*################################## blow private method ##################################*/

    private void executeOnNext(ByteBuf byteBufFromPeer) {
        // 1. cumulate Buffer
        final ByteBuf cumulateBuffer;
        if (this.cumulateBuffer == null) {
            cumulateBuffer = byteBufFromPeer;
        } else {
            cumulateBuffer = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                    byteBufFromPeer.alloc(), this.cumulateBuffer, byteBufFromPeer);
        }
        this.cumulateBuffer = cumulateBuffer;
        if (drainReceiver(cumulateBuffer)) {
            this.upstream.request(1L);
        }
    }

    private boolean drainReceiver(@Nullable ByteBuf cumulateBuffer) {

        if (cumulateBuffer == null) {
            return true;
        }
        // 1. invoke receiver
        final MySQLReceiver receiver = this.receiverQueue.peek();
        if (receiver == null) {
            return true;
        }
        final int readableBytes = cumulateBuffer.readableBytes();
        final ByteBuf newCumulateBuffer;
        if (receiver instanceof MonoMySQLReceiver) {
            newCumulateBuffer = onNextForMono((MonoMySQLReceiver) receiver, cumulateBuffer);
        } else if (receiver instanceof FluxMySQLReceiver) {
            newCumulateBuffer = onNextForFlux((FluxMySQLReceiver) receiver, cumulateBuffer);
        } else {
            // never here
            throw new IllegalStateException("receiver unknown.");
        }
        // 2. handle newCumulateBuffer
        if (newCumulateBuffer != null && !newCumulateBuffer.isReadable()) {
            newCumulateBuffer.release();
            this.cumulateBuffer = null;
        } else {
            this.cumulateBuffer = newCumulateBuffer;
        }
        return newCumulateBuffer == cumulateBuffer && cumulateBuffer.readableBytes() != readableBytes;
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
        if (this.complete > 0) {
            receiver.onError(new JdbdMySQLException("Cannot subscribe MySQL packet because connection closed."));
        }
        final Queue<MySQLReceiver> receiverQueue = this.receiverQueue;
        if (receiverQueue.offer(receiver)) {
            if (receiver == receiverQueue.peek()) {
                drainReceiver(this.cumulateBuffer);
            }
        } else {
            receiver.onError(new JdbdMySQLException(
                    "Cannot subscribe MySQL packet because queue limit is exceeded"));
        }
    }

    @Nullable
    private ByteBuf onNextForMono(MonoMySQLReceiver receiver, ByteBuf cumulateBuffer) {
        final ByteBuf decodedBuf = receiver.decode(cumulateBuffer);
        if (decodedBuf == null) {
            return cumulateBuffer;
        }
        this.receiverQueue.poll();
        receiver.success(decodedBuf);
        return decodedBuf == cumulateBuffer ? null : cumulateBuffer;

    }

    @Nullable
    private ByteBuf onNextForFlux(FluxMySQLReceiver receiver, final ByteBuf cumulateBuffer) {
        List<ByteBuf> decodedList = new ArrayList<>();
        final MySQLReceiver removeReceive;
        if (receiver.decode(cumulateBuffer, decodedList)) {
            removeReceive = this.receiverQueue.poll();
        } else {
            removeReceive = null;
        }

        for (ByteBuf decodeBuf : decodedList) {
            receiver.onNext(decodeBuf);
        }

        if (removeReceive != null) {
            receiver.onComplete();
        }

        final ByteBuf newCumulateBuffer;
        if (decodedList.size() == 1 && decodedList.get(0) == cumulateBuffer) {
            newCumulateBuffer = null;
        } else {
            newCumulateBuffer = cumulateBuffer;
        }
        return newCumulateBuffer;
    }


    /*################################## blow private method ##################################*/

    @Nullable
    private static ByteBuf parseMySQLPacket(final ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        if (readableBytes < PacketUtils.HEADER_SIZE) {
            return null;
        }

        final int packetLength;
        packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex());

        final ByteBuf packetBuf;
        if (readableBytes < packetLength) {
            packetBuf = null;
        } else if (readableBytes == packetLength) {
            packetBuf = cumulateBuffer;
        } else {
            packetBuf = cumulateBuffer.readRetainedSlice(packetLength);
        }
        return packetBuf;
    }


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
        boolean decode(ByteBuf cumulateBuffer, List<ByteBuf> decodedList);

        void onNext(ByteBuf byteBuf);

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

        private final BiFunction<ByteBuf, List<ByteBuf>, Boolean> decoder;

        private FluxMonoMySQLReceiver(FluxSink<ByteBuf> sink, BiFunction<ByteBuf, List<ByteBuf>, Boolean> decoder) {
            this.sink = sink;
            this.decoder = decoder;
        }

        @Override
        public void onError(Throwable t) {
            this.sink.error(t);
        }

        @Override
        public boolean decode(ByteBuf cumulateBuffer, List<ByteBuf> decodedList) {
            return this.decoder.apply(cumulateBuffer, decodedList);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            this.sink.next(byteBuf);
        }

        @Override
        public void onComplete() {
            this.sink.complete();
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
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.actual.onSubscribe(s);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            try {
                this.actual.onNext(byteBuf);
            } finally {
                try {
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
