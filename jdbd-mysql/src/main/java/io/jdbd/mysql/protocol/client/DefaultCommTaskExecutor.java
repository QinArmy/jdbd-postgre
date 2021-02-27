package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.CommunicationTask;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

final class DefaultCommTaskExecutor implements MySQLCommTaskExecutor, CoreSubscriber<ByteBuf>, TaskSignal<ByteBuf> {

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

    static void updateProtocolAdjutant(DefaultCommTaskExecutor executor
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
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCommTaskExecutor.class);

    private final Queue<MySQLTask> taskQueue = Queues.<MySQLTask>small().get();

    private final Connection connection;

    private final EventLoop eventLoop;

    private final ByteBufAllocator allocator;

    private final MySQLTaskAdjutantWrapper taskAdjutant;

    // non-volatile ,all modify in netty EventLoop
    private ByteBuf cumulateBuffer;

    private Subscription upstream;

    private MySQLTask currentTask;

    private int serverStatus;

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

    @Override
    public Mono<Void> terminate(CommunicationTask<ByteBuf> task) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> sendPacket(CommunicationTask<ByteBuf> task) {
        return Mono.empty();
    }

    @Override
    public boolean canSignal() {
        return false;
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
        MySQLTask task = this.currentTask;
        if (task != null) {
            task.onChannelClose();
        }
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
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommunicationTask."));
            }
            while ((task = this.taskQueue.poll()) != null) {
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommunicationTask."));
            }
        } else {
            // TODO optimize handle netty Handler error.
            MySQLTask task = this.currentTask;
            if (task != null) {
                this.currentTask = null;
                task.error(new JdbdMySQLException(e, "TCP connection close,cannot execute CommunicationTask."));
            }

        }
    }


    /**
     * must invoke in {@link #eventLoop}
     */
    private void drainToTask() {
        final ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            return;
        }
        MySQLTask currentTask;
        while ((currentTask = this.currentTask) != null) {
            if (!cumulateBuffer.isReadable()) {
                break;
            }

            if (currentTask.decode(cumulateBuffer, this::updateServerStatus)) {
                this.currentTask = null; // current task end.
                if (!startHeadIfNeed()) {  // start next task
                    break;
                }
            } else {
                Publisher<ByteBuf> bufPublisher = currentTask.moreSendPacket();
                if (bufPublisher != null) {
                    // send packet
                    sendPacket(currentTask, bufPublisher);
                }
                break;
            }
        }


    }

    private void updateServerStatus(Object serversStatus) {
        this.serverStatus = (Integer) serversStatus;
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
        boolean needDecode = false;
        while ((currentTask = this.taskQueue.poll()) != null) {
            if (currentTask.getTaskPhase() != CommunicationTask.TaskPhase.SUBMITTED) {
                currentTask.error(new JdbdMySQLException("CommunicationTask[%s] phase isn't SUBMITTED,cannot start."
                        , currentTask));
                continue;
            }
            this.currentTask = currentTask;
            Publisher<ByteBuf> bufPublisher = currentTask.start(this);
            if (bufPublisher != null) {
                // send packet
                sendPacket(currentTask, bufPublisher);
            } else {
                this.upstream.request(128L);
                needDecode = true;
            }
            break;
        }
        return needDecode;
    }

    private void sendPacket(MySQLTask headTask, Publisher<ByteBuf> bufPublisher) {
        Mono.from(this.connection.outbound().send(bufPublisher))
                .doOnError(e -> removeTaskOnStartFailure(headTask, e))
                .subscribe(v -> this.upstream.request(Long.MAX_VALUE))
        ;
    }


    /**
     * @see #sendPacket(MySQLTask, Publisher)
     */
    private void removeTaskOnStartFailure(MySQLTask headTask, Throwable e) {
        headTask.error(MySQLExceptions.wrapSQLExceptionIfNeed(e)); // emit error
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


    private void syncPushTask(MySQLTask task, Consumer<Boolean> offerCall) throws JdbdMySQLException {
        if (!this.eventLoop.inEventLoop()) {
            throw new JdbdMySQLException("Current thread not in EventLoop.");
        }
        if (!this.connection.channel().isActive()) {
            throw new JdbdMySQLException("Cannot summit CommunicationTask because TCP connection closed.");
        }
        if (this.taskQueue.offer(task)) {
            offerCall.accept(Boolean.TRUE);
            startHeadIfNeed();
            drainToTask();
        } else {
            offerCall.accept(Boolean.FALSE);
        }
    }


    private static JdbdMySQLException createChannelCloseError() {
        return new JdbdMySQLException("Cannot summit CommunicationTask because TCP connection closed.");
    }

    private static JdbdMySQLException createQueueOverflow() {
        return new JdbdMySQLException("Cannot submit CommunicationTask because queue limit is exceeded");
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
        public ByteBuf createByteBuffer(int initialPayloadCapacity) {
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
        public ByteBufAllocator alloc() {
            return DefaultCommTaskExecutor.this.allocator;
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
        public void syncSubmitTask(CommunicationTask<?> task, Consumer<Boolean> offerCall) throws JdbdMySQLException {
            DefaultCommTaskExecutor.this.syncPushTask((MySQLTask) task, offerCall);
        }

        @Override
        public void execute(Runnable runnable) {
            DefaultCommTaskExecutor.this.eventLoop.execute(runnable);
        }

        @Override
        public boolean isAutoCommit() throws JdbdMySQLException {
            if (this.inEventLoop()) {
                return (DefaultCommTaskExecutor.this.serverStatus & ClientProtocol.SERVER_STATUS_AUTOCOMMIT) != 0;
            }
            throw new JdbdMySQLException("Current not in EventLoop.");
        }

        @Nullable
        @Override
        public Integer getServerStatus() throws JdbdMySQLException {
            if (this.inEventLoop()) {
                int status = DefaultCommTaskExecutor.this.serverStatus;
                Integer serverStatus;
                if (status == 0) {
                    serverStatus = status;
                } else {
                    serverStatus = null;
                }
                return serverStatus;
            }
            throw new JdbdMySQLException("Current not in EventLoop.");
        }

        @Override
        public Server obtainServer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MySQLCommTaskExecutor obtainCommTaskExecutor() {
            return DefaultCommTaskExecutor.this;
        }

        @Override
        public MySQLStatement parse(String singleSql) throws SQLException {
            return null;
        }

        @Override
        public boolean isSingleStmt(String sql) throws SQLException {
            return false;
        }

        @Override
        public boolean isMultiStmt(String sql) throws SQLException {
            return false;
        }
    }

}
