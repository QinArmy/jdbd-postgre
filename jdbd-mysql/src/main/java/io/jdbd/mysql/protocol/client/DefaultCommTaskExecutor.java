package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.CommTask;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

final class DefaultCommTaskExecutor implements MySQLCommTaskExecutor, CoreSubscriber<ByteBuf> {

    static Mono<DefaultCommTaskExecutor> create(HostInfo hostInfo, EventLoopGroup eventLoopGroup) {
        return TcpClient.create()
                .runOn(eventLoopGroup)
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .connect()
                .map(DefaultCommTaskExecutor::new)
                ;
    }

    static void setProtocolAdjutant(DefaultCommTaskExecutor executor
            , ClientConnectionProtocolImpl adjutant) {
        boolean success = false;
        synchronized (executor.taskAdjutant) {
            if (executor.taskAdjutant.updateCounter.compareAndSet(0, 1)) {
                executor.taskAdjutant.adjutant = adjutant;
                success = true;
            }
        }
        if (!success) {
            throw new IllegalStateException("Only once update taskAdjutant allowed. ");
        }
    }

    static MySQLCommTaskExecutor updateProtocolAdjutant(DefaultCommTaskExecutor executor
            , ClientCommandProtocolImpl adjutant) {
        boolean success = false;
        synchronized (executor.taskAdjutant) {
            if (executor.taskAdjutant.updateCounter.compareAndSet(1, 2)) {
                executor.taskAdjutant.adjutant = adjutant;
                success = true;
            }
        }
        if (!success) {
            throw new IllegalStateException("Only once update taskAdjutant allowed. ");
        }
        return executor;
    }

    private final Queue<MySQLTask> taskQueue = Queues.<MySQLTask>small().get();

    private final Connection connection;

    private final EventLoop eventLoop;

    private final ByteBufAllocator allocator;

    private final MySQLTaskAdjutantWrapper taskAdjutant;

    // non-volatile ,all modify in netty EventLoop
    private ByteBuf cumulateBuffer;

    private Subscription upstream;

    private MySQLTask currentTask;

    private DefaultCommTaskExecutor(Connection connection) {
        this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();
        this.allocator = connection.channel().alloc();
        this.taskAdjutant = new MySQLTaskAdjutantWrapper(null);

        connection.inbound()
                .receive()
                .retain() // for cumulate
                .subscribe(this);
    }


    @Override
    public MySQLTaskAdjutant getAdjutant() {
        return this.taskAdjutant;
    }


    @Override
    public void onSubscribe(Subscription s) {
        this.upstream = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf byteBufFromPeer) {
        if (this.eventLoop.inEventLoop()) {
            if (byteBufFromPeer == this.cumulateBuffer) {
                // due to byteBufFromPeer.release()
                throw new JdbdMySQLException("previous cumulateBuffer handle error.");
            }
            doOnNextInEventLoop(byteBufFromPeer);
        } else {
            this.eventLoop.execute(() -> doOnNextInEventLoop(byteBufFromPeer));
        }
    }

    @Override
    public void onError(Throwable t) {
        if (this.eventLoop.inEventLoop()) {
            doOnErrorInEventLoop(t);
        } else {
            this.eventLoop.execute(() -> doOnErrorInEventLoop(t));
        }
    }

    @Override
    public void onComplete() {
        if (this.eventLoop.inEventLoop()) {
            doOnCompleteInEventLoop();
        } else {
            this.eventLoop.execute(this::doOnCompleteInEventLoop);
        }
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnNextInEventLoop(ByteBuf byteBufFromPeer) {
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

        drainToTask();
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnCompleteInEventLoop() {
        MySQLTask task;
        while ((task = this.taskQueue.poll()) != null) {
            task.onChannelClose();
        }
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnErrorInEventLoop(Throwable e) {
        if (!this.connection.channel().isActive()) {
            MySQLTask task = this.currentTask;
            if (task != null) {
                this.currentTask = null;
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommTask."));
            }
            while ((task = this.taskQueue.poll()) != null) {
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommTask."));
            }
        } else {
            // TODO optimize handle netty Handler error.
            MySQLTask task = this.currentTask;
            if (task != null) {
                this.currentTask = null;
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommTask."));
            }

        }
    }


    /**
     * must invoke in {@link #eventLoop}
     */
    private void drainToTask() {

        final ByteBuf cumulateBuffer = this.cumulateBuffer;
        MySQLTask currentTask;
        while ((currentTask = this.currentTask) != null) {
            if (cumulateBuffer == null || !cumulateBuffer.isReadable()) {
                break;
            }
            if (currentTask.decode(cumulateBuffer)) {
                if (!startHeadIfNeed()) {  // start next task
                    break;
                }
            } else {
                ByteBuf packetBuf;
                while ((packetBuf = currentTask.moreSendPacket()) != null) {
                    // send packet
                    sendPacket(currentTask, packetBuf);
                }
                Path path;
                while ((path = currentTask.moreSendFile()) != null) {
                    sendFile(path);
                }
                break;
            }
        }

    }

    /**
     * must invoke in {@link #eventLoop}
     *
     * @return true : need decode
     */
    private boolean startHeadIfNeed() {
        MySQLTask currentTask = this.currentTask;
        if (currentTask != null) {
            return false;
        }
        currentTask = this.taskQueue.poll();
        if (currentTask == null) {
            return false;
        }
        this.currentTask = currentTask;
        final MySQLTask headTask = currentTask;

        ByteBuf packetBuf = headTask.start();
        boolean needDecode = false;
        if (packetBuf != null) {
            // send packet
            sendPacket(headTask, packetBuf);
        } else {
            this.upstream.request(128L);
            needDecode = true;
        }
        return needDecode;
    }

    private void sendPacket(MySQLTask headTask, ByteBuf packetBuf) {
        Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuf)))
                .doOnError(e -> removeTaskOnStartFailure(headTask, e))
                .subscribe(v -> this.upstream.request(512L))
        ;
    }

    private void sendFile(Path path) {
        // TODO finish
    }

    /**
     * @see #sendPacket(MySQLTask, ByteBuf)
     */
    private void removeTaskOnStartFailure(MySQLTask headTask, Throwable e) {
        headTask.error(MySQLExceptionUtils.wrapSQLExceptionIfNeed(e)); // emit error
        if (this.eventLoop.inEventLoop()) {
            if (this.currentTask == headTask) {
                this.currentTask = null;
            }
        } else {
            this.eventLoop.execute(() -> {
                if (this.currentTask == headTask) {
                    this.currentTask = null;
                }
            });
        }
    }

    private Mono<Void> pushTask(MySQLTask task) {
        Mono<Void> mono;
        if (this.eventLoop.inEventLoop()) {
            if (!this.connection.channel().isActive()) {
                mono = Mono.error(createChannelCloseError());
            } else if (this.taskQueue.offer(task)) {
                if (startHeadIfNeed()) {
                    drainToTask();
                }
                mono = Mono.empty();
            } else {
                mono = Mono.error(createQueueOverflow());
            }
        } else {
            mono = Mono.create(sink -> this.eventLoop.execute(() -> {
                if (!this.connection.channel().isActive()) {
                    sink.error(createChannelCloseError());
                } else if (this.taskQueue.offer(task)) {
                    sink.success();
                    if (startHeadIfNeed()) {
                        drainToTask();
                    }
                } else {
                    sink.error(createQueueOverflow());
                }
            }));
        }

        return mono;
    }


    private static JdbdMySQLException createChannelCloseError() {
        return new JdbdMySQLException("Cannot summit CommTask because TCP connection closed.");
    }

    private static JdbdMySQLException createQueueOverflow() {
        return new JdbdMySQLException("Cannot submit CommTask because queue limit is exceeded");
    }


    /*################################## blow private method ##################################*/

    private final class MySQLTaskAdjutantWrapper implements MySQLTaskAdjutant {

        private final AtomicInteger updateCounter = new AtomicInteger(0);

        private ClientProtocolAdjutant adjutant;

        private MySQLTaskAdjutantWrapper(@Nullable ClientProtocolAdjutant adjutant) {
            this.adjutant = adjutant;
        }

        @Override
        public ByteBuf createPacketBuffer(int initialPayloadCapacity) {
            ByteBuf packetBuffer = DefaultCommTaskExecutor.this.allocator.buffer(
                    PacketUtils.HEADER_SIZE + initialPayloadCapacity);
            packetBuffer.writeZero(PacketUtils.HEADER_SIZE);
            return packetBuffer;

        }

        @Override
        public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
            return DefaultCommTaskExecutor.this.allocator.buffer(initialPayloadCapacity);
        }

        @Override
        public int obtainMaxBytesPerCharClient() {
            return this.adjutant.obtainMaxBytesPerCharClient();
        }

        @Override
        public Charset obtainCharsetClient() {
            return this.adjutant.obtainCharsetClient();
        }

        @Override
        public Charset obtainCharsetResults() {
            return this.adjutant.obtainCharsetResults();
        }

        @Override
        public int obtainNegotiatedCapability() {
            return this.adjutant.obtainNegotiatedCapability();
        }

        @Override
        public Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
            return this.adjutant.obtainCustomCollationMap();
        }

        @Override
        public ZoneOffset obtainZoneOffsetDatabase() {
            return this.adjutant.obtainZoneOffsetDatabase();
        }

        @Override
        public HandshakeV10Packet obtainHandshakeV10Packet() {
            return this.adjutant.obtainHandshakeV10Packet();
        }

        @Override
        public HostInfo obtainHostInfo() {
            return this.adjutant.obtainHostInfo();
        }

        @Override
        public ZoneOffset obtainZoneOffsetClient() {
            return this.adjutant.obtainZoneOffsetClient();
        }

        @Override
        public boolean inEventLoop() {
            return DefaultCommTaskExecutor.this.eventLoop.inEventLoop();
        }

        @Override
        public Mono<Void> submitTask(CommTask<?> task) {
            return DefaultCommTaskExecutor.this.pushTask((MySQLTask) task);
        }

        @Override
        public MySQLCommTaskExecutor obtainCommTaskExecutor() {
            return DefaultCommTaskExecutor.this;
        }
    }

}
