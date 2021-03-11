package io.jdbd.vendor.task;

import io.jdbd.JdbdException;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.netty.Connection;
import reactor.netty.NettyPipeline;
import reactor.netty.tcp.SslProvider;
import reactor.util.concurrent.Queues;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class CommunicationTaskExecutor<T extends TaskAdjutant> implements CoreSubscriber<ByteBuf>
        , TaskExecutor<T> {

    protected final Connection connection;

    protected final EventLoop eventLoop;

    protected final ByteBufAllocator allocator;

    protected final T taskAdjutant;

    //private final MySQLTaskAdjutantWrapper taskAdjutant;

    // non-volatile ,all modify in netty EventLoop
    protected ByteBuf cumulateBuffer;

    private final Queue<CommunicationTask> taskQueue = Queues.<CommunicationTask>small().get();

    private final MorePacketSignal morePacketSignal;

    private Subscription upstream;

    private CommunicationTask currentTask;

    private Throwable taskErrorMethodError;


    protected CommunicationTaskExecutor(Connection connection) {
        this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();
        this.allocator = connection.channel().alloc();
        this.morePacketSignal = new MorePacketSingleImpl(this);

        this.taskAdjutant = createTaskAdjutant();
        connection.inbound()
                .receive()
                .retain() // for cumulate
                .subscribe(this);

    }

    protected abstract Logger obtainLogger();


    @Override
    public final T getAdjutant() {
        return this.taskAdjutant;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.upstream = s;
        s.request(Long.MAX_VALUE);
    }


    @Override
    public final void onNext(final ByteBuf byteBufFromPeer) {
        if (this.eventLoop.inEventLoop()) {
            doOnNextInEventLoop(byteBufFromPeer);
        } else {
            if (obtainLogger().isDebugEnabled()) {
                Logger LOG = obtainLogger();
                LOG.debug("{} onNext(),current thread not in EventLoop.", this);
            }
            this.eventLoop.execute(() -> doOnNextInEventLoop(byteBufFromPeer));
        }
    }

    @Override
    public final void onError(Throwable t) {
        if (this.eventLoop.inEventLoop()) {
            doOnErrorInEventLoop(t);
        } else {
            this.eventLoop.execute(() -> doOnErrorInEventLoop(t));
        }
    }

    @Override
    public final void onComplete() {
        if (this.eventLoop.inEventLoop()) {
            doOnCompleteInEventLoop();
        } else {
            this.eventLoop.execute(this::doOnCompleteInEventLoop);
        }
    }

    /*################################## blow protected method ##################################*/

    /**
     * must invoke in {@link #eventLoop}
     */
    protected final void drainToTask() {
        final ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            return;
        }
        CommunicationTask currentTask;
        while ((currentTask = this.currentTask) != null) {
            if (!cumulateBuffer.isReadable()) {
                break;
            }
            if (currentTask.decode(cumulateBuffer, this::updateServerStatus)) {
                if (currentTask instanceof ConnectionTask) {
                    ConnectionTask connectionTask = (ConnectionTask) currentTask;
                    if (connectionTask.disconnect()) {
                        disconnection();
                        return;
                    }
                }
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


    protected abstract void updateServerStatus(Object serverStatus);

    protected abstract T createTaskAdjutant();

    protected abstract HostInfo<?> obtainHostInfo();


    /*################################## blow private method ##################################*/

    final void syncPushTask(CommunicationTask task, Consumer<Boolean> offerCall)
            throws IllegalStateException {
        if (!this.eventLoop.inEventLoop()) {
            throw new IllegalStateException("Current thread not in EventLoop.");
        }
        if (!this.connection.channel().isActive()) {
            throw new IllegalStateException("Cannot summit CommunicationTask because TCP connection closed.");
        }
        if (this.taskQueue.offer(task)) {
            offerCall.accept(Boolean.TRUE);
            if (startHeadIfNeed()) {
                drainToTask();
            }
        } else {
            offerCall.accept(Boolean.FALSE);
        }
    }


    /**
     * @see #onNext(ByteBuf)
     */
    private void doOnNextInEventLoop(final ByteBuf byteBufFromPeer) {
        if (obtainLogger().isTraceEnabled()) {
            obtainLogger().trace("doOnNextInEventLoop() readableBytes={}", byteBufFromPeer.readableBytes());
        }
        if (byteBufFromPeer == this.cumulateBuffer) {
            // subclass bug
            throw new IllegalStateException("previous cumulateBuffer handle error.");
        }
        //  cumulate Buffer
        ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            cumulateBuffer = byteBufFromPeer;
        } else {
            cumulateBuffer = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                    this.allocator, cumulateBuffer, byteBufFromPeer);
        }
        this.cumulateBuffer = cumulateBuffer;
        drainToTask();
    }

    private void doOnErrorInEventLoop(Throwable e) {
        obtainLogger().debug("channel upstream error.");

        if (!this.connection.channel().isActive()) {
            CommunicationTask task = this.currentTask;
            final JdbdException exception = JdbdExceptions.wrap(e
                    , "TCP connection close,cannot execute CommunicationTask.");
            if (task != null) {
                this.currentTask = null;
                task.error(exception);
            }
            while ((task = this.taskQueue.poll()) != null) {
                task.error(exception);
            }
        } else {
            // TODO optimize handle netty Handler error.
            CommunicationTask task = this.currentTask;
            if (task != null) {
                this.currentTask = null;
                task.error(JdbdExceptions.wrap(e, "Channel upstream throw error."));
            }

        }
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnCompleteInEventLoop() {
        Logger LOG = obtainLogger();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection close.");
        }
        CommunicationTask task = this.currentTask;
        if (task != null) {
            task.onChannelClose();
        }
        while ((task = this.taskQueue.poll()) != null) {
            task.onChannelClose();
        }
    }

    /**
     * must invoke in {@link #eventLoop}
     *
     * @return true : need decode
     */
    private boolean startHeadIfNeed() {
        CommunicationTask currentTask = this.currentTask;
        if (currentTask != null) {
            return false;
        }

        Throwable taskErrorMethodError = this.taskErrorMethodError;

        boolean needDecode = false;
        while ((currentTask = this.taskQueue.poll()) != null) {

            if (currentTask.getTaskPhase() != CommunicationTask.TaskPhase.SUBMITTED) {
                currentTask.error(new TaskStatusException("CommunicationTask[%s] phase isn't SUBMITTED,cannot start."
                        , currentTask));
                continue;
            }
            if (taskErrorMethodError != null) {
                currentTask.error(JdbdExceptions.wrap(taskErrorMethodError
                        , "last task error(Throwable) method throw error."));
                // TODO optimize handle last task error.
                this.taskErrorMethodError = null;
                taskErrorMethodError = null;
                continue;
            }

            this.currentTask = currentTask;
            if (currentTask instanceof ConnectionTask) {
                ConnectionTask connectionTask = ((ConnectionTask) currentTask);
                if (connectionTask.disconnect()) {
                    disconnection();
                    return false;
                }
                connectionTask.connectSignal(this::addSslHandler);
            }
            Publisher<ByteBuf> bufPublisher;
            try {
                // invoke task start() method.
                bufPublisher = currentTask.start(this.morePacketSignal);
            } catch (Throwable e) {
                handleTaskError(currentTask, new TaskStatusException(e, "%s start failure.", currentTask));
                continue;
            }
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

    private void disconnection() {
        if (this.currentTask instanceof ConnectionTask) {
            Logger LOG = obtainLogger();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Task[{}] demand disconnect.", this.currentTask);
            }
            this.connection.channel()
                    .close();
        } else {
            throw new IllegalStateException(String.format("Current %s[%s] isn't %s,reject disconnect."
                    , CommunicationTask.class.getName(), this.currentTask, ConnectionTask.class.getName()));
        }
    }

    /**
     * @see ConnectionTask#connectSignal(Function)
     */
    private Mono<Void> addSslHandler(final Object sslObject) {
        return Mono.create(sink -> {
            if (this.eventLoop.inEventLoop()) {
                doAddSslHandler(sink, sslObject);
            } else {
                this.eventLoop.execute(() -> doAddSslHandler(sink, sslObject));
            }

        });
    }

    private void doAddSslHandler(final MonoSink<Void> sink, final Object sslObject) {

        final Logger LOG = obtainLogger();
        final boolean traceEnabled = LOG.isTraceEnabled();
        final ChannelPipeline pipeline = this.connection.channel().pipeline();

        addSslHandshakeSuccessListener(sink, pipeline, LOG);

        if (sslObject instanceof SslProvider) {
            SslProvider sslProvider = (SslProvider) sslObject;
            HostInfo<?> hostInfo = obtainHostInfo();
            InetSocketAddress address = InetSocketAddress.createUnresolved(hostInfo.getHost(), hostInfo.getPort());
            sslProvider.addSslHandler(this.connection.channel(), address, traceEnabled);
        } else if (sslObject instanceof SslHandler) {
            SslHandler sslHandler = (SslHandler) sslObject;
            if (pipeline.get(NettyPipeline.ProxyHandler) != null) {
                pipeline.addAfter(NettyPipeline.ProxyHandler, NettyPipeline.SslHandler, sslHandler);
            } else {
                pipeline.addFirst(NettyPipeline.SslHandler, sslHandler);
            }
        }

        this.upstream.request(512L);
    }

    /**
     * @see #doAddSslHandler(MonoSink, Object)
     */
    private void addSslHandshakeSuccessListener(MonoSink<Void> sink, ChannelPipeline pipeline, Logger LOG) {
        final String handlerName = "jdbd.mysql.ssl.handshake.success.event.handler";
        pipeline.addBefore(NettyPipeline.ReactiveBridge, handlerName, new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                final boolean sslHandshakeSuccess;
                if (evt instanceof SslHandshakeCompletionEvent) {
                    SslHandshakeCompletionEvent event = (SslHandshakeCompletionEvent) evt;
                    sslHandshakeSuccess = event.isSuccess();
                    if (sslHandshakeSuccess) {
                        LOG.debug("SSL handshake success");
                        sink.success();
                        sendPacketAfterSslHandshakeSuccess();
                    }
                } else {
                    sslHandshakeSuccess = false;
                }
                super.userEventTriggered(ctx, evt);

                if (sslHandshakeSuccess) {
                    ChannelPipeline pipeline = ctx.pipeline();
                    if (pipeline.context(this) != null) {
                        pipeline.remove(this);
                    }
                }

            }
        });
    }

    /**
     * @see #addSslHandshakeSuccessListener(MonoSink, ChannelPipeline, Logger)
     */
    private void sendPacketAfterSslHandshakeSuccess() {
        final CommunicationTask currentTask = this.currentTask;
        if (currentTask instanceof ConnectionTask) {
            Publisher<ByteBuf> packetPublisher = currentTask.moreSendPacket();
            if (packetPublisher != null) {
                sendPacket(currentTask, packetPublisher);
            }

        }
    }

    private void doSendPacketSignal(final MonoSink<Void> sink, final CommunicationTask signalTask) {
        if (obtainLogger().isDebugEnabled()) {
            obtainLogger().debug("{} send packet signal", signalTask);
        }
        if (signalTask == this.currentTask) {
            Publisher<ByteBuf> packetPublisher;
            try {
                packetPublisher = signalTask.moreSendPacket();
            } catch (Throwable e) {
                sink.error(new TaskStatusException(e, "%s moreSendPacket() method throw error.", signalTask));
                return;
            }
            if (packetPublisher != null) {
                sendPacket(signalTask, packetPublisher);
            }
            sink.success();
        } else {
            sink.error(new IllegalArgumentException(String.format("task[%s] isn't current task.", signalTask)));
        }
    }

    private void sendPacket(final CommunicationTask headTask, final Publisher<ByteBuf> packetPublisher) {
        Mono.from(this.connection.outbound().send(packetPublisher))
                .doOnError(e -> {
                    if (this.eventLoop.inEventLoop()) {
                        handleTaskError(headTask, e);
                    } else {
                        this.eventLoop.execute(() -> handleTaskError(headTask, e));
                    }
                })
                .doOnSuccess(v -> {
                    if (this.eventLoop.inEventLoop()) {
                        handleTaskPacketSendSuccess(headTask);
                    } else {
                        obtainLogger().trace("send packet success,but current thread not in EventLoop.");
                        this.eventLoop.execute(() -> handleTaskPacketSendSuccess(headTask));
                    }
                })
                .subscribe(v -> this.upstream.request(128L))
        ;
    }

    /**
     * @see #sendPacket(CommunicationTask, Publisher)
     * @see #handleTaskPacketSendSuccess(CommunicationTask)
     * @see #startHeadIfNeed()
     * @see #doSendPacketSignal(MonoSink, CommunicationTask)
     */
    private void handleTaskError(final CommunicationTask task, final Throwable e) {
        try {
            task.error(JdbdExceptions.wrap(e));
            currentTaskEndIfNeed(task);
        } catch (Throwable te) {
            this.taskErrorMethodError = new TaskStatusException(e, "%s error(Throwable) method throw error.", task);
        }

    }

    private void handleTaskPacketSendSuccess(final CommunicationTask task) {
        boolean hasMorePacket = false;
        try {
            hasMorePacket = task.onSendSuccess();
        } catch (Throwable e) {
            handleTaskError(task, new TaskStatusException(e, "%s onSendSuccess() method throw error.", task));
        }

        if (hasMorePacket) {
            Publisher<ByteBuf> publisher = null;
            try {
                publisher = task.moreSendPacket();
            } catch (Throwable e) {
                handleTaskError(task, new TaskStatusException(e, "%s moreSendPacket() method throw error.", task));
            }
            if (publisher != null) {
                sendPacket(task, publisher);
            }
        } else if (task instanceof ConnectionTask) {
            ConnectionTask connectionTask = (ConnectionTask) task;
            if (connectionTask == this.currentTask && connectionTask.disconnect()) {
                disconnection();
            }
        }

    }

    private void currentTaskEndIfNeed(final CommunicationTask task) {
        if (task == this.currentTask) {

            ByteBuf cumulate = this.cumulateBuffer;
            if (cumulate != null) {
                if (cumulate.isReadable()) {
                    cumulate.readerIndex(cumulate.writerIndex());
                }
                cumulate.release();
                this.cumulateBuffer = null;
            }

            this.currentTask = null;

            if (startHeadIfNeed()) {
                drainToTask();
            }

        }


    }

    /*################################## blow private static class ##################################*/

    protected static abstract class AbstractTaskAdjutant implements TaskAdjutant {

        private final CommunicationTaskExecutor<?> taskExecutor;

        protected AbstractTaskAdjutant(CommunicationTaskExecutor<?> taskExecutor) {
            this.taskExecutor = taskExecutor;
        }

        @Override
        public final boolean inEventLoop() {
            return this.taskExecutor.eventLoop.inEventLoop();
        }

        @Override
        public final void syncSubmitTask(CommunicationTask task, Consumer<Boolean> offerCall)
                throws IllegalStateException {
            this.taskExecutor.syncPushTask(task, offerCall);
        }

        @Override
        public final void execute(Runnable runnable) {
            this.taskExecutor.eventLoop.execute(runnable);
        }

        @Override
        public final ByteBufAllocator allocator() {
            return this.taskExecutor.allocator;
        }

    }

    private static final class MorePacketSingleImpl implements MorePacketSignal {

        private final CommunicationTaskExecutor<?> taskExecutor;

        private MorePacketSingleImpl(CommunicationTaskExecutor<?> taskExecutor) {
            this.taskExecutor = taskExecutor;
        }


        @Override
        public Mono<Void> sendPacket(final CommunicationTask task) {
            return Mono.create(sink -> {
                if (this.taskExecutor.eventLoop.inEventLoop()) {
                    this.taskExecutor.doSendPacketSignal(sink, task);
                } else {
                    this.taskExecutor.eventLoop.execute(() -> this.taskExecutor.doSendPacketSignal(sink, task));
                }
            });
        }
    }


}
