package io.jdbd.vendor.task;

import io.jdbd.JdbdException;
import io.jdbd.SessionCloseException;
import io.jdbd.stmt.TaskQueueOverflowException;
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
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;

public abstract class CommunicationTaskExecutor<T extends TaskAdjutant> implements CoreSubscriber<ByteBuf>
        , TaskExecutor<T> {

    protected final Connection connection;

    protected final EventLoop eventLoop;

    protected final ByteBufAllocator allocator;

    protected final T taskAdjutant;

    private final Queue<CommunicationTask> taskQueue = Queues.<CommunicationTask>small().get();

    private final TaskSignal taskSignal;

    // non-volatile ,all modify in netty EventLoop
    private ByteBuf cumulateBuffer;

    private Subscription upstream;

    private CommunicationTask currentTask;

    private TaskStatusException taskError;

    private int packetIndex = -1;

    private TaskStack taskStack;


    protected CommunicationTaskExecutor(Connection connection) {
        this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();
        this.allocator = connection.channel().alloc();
        this.taskSignal = new TaskSingleImpl(this);

        this.taskAdjutant = createTaskAdjutant();
        connection.inbound()
                .receive()
                .retain() // for cumulate
                .subscribe(this);

    }


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
            final Logger LOG = obtainLogger();
            if (LOG.isDebugEnabled()) {
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
            final Logger LOG = obtainLogger();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} onError(Throwable),current thread not in EventLoop.", this);
            }
            this.eventLoop.execute(() -> doOnErrorInEventLoop(t));
        }
    }

    @Override
    public final void onComplete() {
        if (this.eventLoop.inEventLoop()) {
            doOnCompleteInEventLoop();
        } else {
            final Logger LOG = obtainLogger();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} onComplete(),current thread not in EventLoop.", this);
            }
            this.eventLoop.execute(this::doOnCompleteInEventLoop);
        }
    }

    /*################################## blow protected method ##################################*/

    /**
     * must invoke in {@link #eventLoop}
     *
     * @see #startHeadIfNeed()
     * @see #doOnNextInEventLoop(ByteBuf)
     */
    protected final void drainToTask() {
        final ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null || !cumulateBuffer.isReadable()) {
            return;
        }
        CommunicationTask currentTask = this.currentTask;
        if (currentTask == null) {
            startHeadIfNeed();
            return;
        }
        //1. store packet start index.
        this.packetIndex = cumulateBuffer.readerIndex();
        this.taskStack = TaskStack.DECODE;
        //2. decode packet from database server.
        final boolean taskEnd;
        taskEnd = currentTask.decodePackets(cumulateBuffer, this::updateServerStatus);
        this.taskStack = null;
        if (taskEnd && currentTask instanceof ConnectionTask) {
            ConnectionTask connectionTask = (ConnectionTask) currentTask;
            if (connectionTask.disconnect()) {
                disconnection();
                return;
            }
        }

        final Publisher<ByteBuf> bufPublisher = currentTask.moreSendPacket();
        if (bufPublisher != null) {
            // send packet
            sendPacket(currentTask, bufPublisher)
                    .subscribe();
        }
        if (taskEnd) {
            if (cumulateBuffer.isReadable()) {
                // TODO maybe has notify from server.
                throw new TaskStatusException("Not read all packet,but task end.");
            }
            this.currentTask = null; // current task end.
            // start next task
            startHeadIfNeed();
        }

    }

    protected abstract Logger obtainLogger();


    protected abstract void updateServerStatus(Object serverStatus);

    protected abstract T createTaskAdjutant();

    protected abstract HostInfo<?> obtainHostInfo();

    /**
     * @return true : clear channel complement
     */
    protected abstract boolean clearChannel(ByteBuf cumulateBuffer, Class<? extends CommunicationTask> taskClass);


    /*################################## blow private method ##################################*/

    private void syncPushTask(final CommunicationTask task, final Consumer<Void> consumer) {
        if (!this.eventLoop.inEventLoop()) {
            throw new IllegalStateException("Current thread not in EventLoop.");
        }
        if (task.getTaskPhase() != null) {
            String message = String.format("%s TaskPhase[%s] isn't null.", task, task.getTaskPhase());
            throw new IllegalArgumentException(message);
        }
        if (!this.connection.channel().isActive()) {
            throw new SessionCloseException("Session closed");

        }
        if (this.taskQueue.offer(task)) {
            consumer.accept(null);
            startHeadIfNeed();
        } else {
            throw new TaskQueueOverflowException("Communication task queue overflow,cant' execute task.");
        }

    }


    /**
     * @see #onNext(ByteBuf)
     */
    private void doOnNextInEventLoop(final ByteBuf byteBufFromPeer) {
        final Logger LOG = obtainLogger();
        if (LOG.isTraceEnabled()) {
            LOG.trace("doOnNextInEventLoop() readableBytes={}", byteBufFromPeer.readableBytes());
        }

        //1. merge  cumulate Buffer
        ByteBuf cumulateBuffer = this.cumulateBuffer;

        if (byteBufFromPeer == cumulateBuffer) {
            //  bug
            throw new IllegalStateException("previous cumulateBuffer handle error.");
        }
        if (cumulateBuffer == null) {
            cumulateBuffer = byteBufFromPeer;
        } else {
            cumulateBuffer = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                    this.allocator, cumulateBuffer, byteBufFromPeer);
        }

        this.cumulateBuffer = cumulateBuffer;

        if (this.taskError == null) {
            try {
                this.packetIndex = cumulateBuffer.readerIndex();
                //2. drain packet to task.
                drainToTask();
            } catch (TaskStatusException e) {
                this.taskError = e;
                Objects.requireNonNull(this.currentTask, "this.currentTask")
                        .errorEvent(e); //invoke error method and ignore action
                handleTaskStatusException();
                return;
            } finally {
                this.taskStack = null;
            }
            cumulateBuffer = this.cumulateBuffer;
            //3. release cumulateBuffer
            if (!cumulateBuffer.isReadable()) {
                cumulateBuffer.release();
                this.cumulateBuffer = null;
            }
        } else {
            try {
                handleTaskStatusException();
            } finally {
                this.taskStack = null;
            }
        }

    }

    /**
     * @see #doOnNextInEventLoop(ByteBuf)
     */
    private void handleTaskStatusException() {
        Objects.requireNonNull(this.taskError, "this.taskError");
        final CommunicationTask currentTask = Objects.requireNonNull(this.currentTask, "this.currentTask");

        final ByteBuf cumulateBuffer = Objects.requireNonNull(this.cumulateBuffer, "this.cumulateBuffer");
        cumulateBuffer.markReaderIndex();
        cumulateBuffer.readerIndex(this.packetIndex);
        if (this.clearChannel(cumulateBuffer, currentTask.getClass())) {
            if (cumulateBuffer.isReadable()) {
                throw new TaskExecutorException(String.format("%s clearChannel method error.", this));
            }
            cumulateBuffer.release();
            this.cumulateBuffer = null;
            this.currentTask = null;
            this.taskError = null;

            Publisher<ByteBuf> publisher = currentTask.moreSendPacket();
            if (publisher != null) {
                sendPacket(currentTask, publisher)
                        .subscribe();
            }
            //start next task
            startHeadIfNeed();
        } else {
            this.packetIndex = cumulateBuffer.readerIndex();
        }


    }

    private void doOnErrorInEventLoop(Throwable e) {
        obtainLogger().debug("channel channel error.");
        this.taskStack = TaskStack.ERROR;
        try {
            if (!this.connection.channel().isActive()) {
                CommunicationTask task = this.currentTask;
                final JdbdException exception = JdbdExceptions.wrap(e
                        , "TCP connection close,cannot execute CommunicationTask.");
                if (task != null) {
                    this.currentTask = null;
                    task.errorEvent(exception);
                }
                while ((task = this.taskQueue.poll()) != null) {
                    task.errorEvent(exception);
                }
            } else {
                // TODO optimize handle netty Handler error.
                CommunicationTask task = this.currentTask;
                if (task != null) {
                    this.currentTask = null;
                    task.errorEvent(JdbdExceptions.wrap(e, "Channel upstream throw error."));
                }

            }
        } finally {
            this.taskStack = null;
        }

    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnCompleteInEventLoop() {
        final Logger LOG = obtainLogger();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection close.");
        }
        CommunicationTask task = this.currentTask;
        if (task != null) {
            task.channelCloseEvent();
        }
        while ((task = this.taskQueue.poll()) != null) {
            task.channelCloseEvent();
        }
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void startHeadIfNeed() {
        CommunicationTask currentTask = this.currentTask;
        if (currentTask != null) {
            return;
        }

        if ((currentTask = this.taskQueue.poll()) != null) {
            this.currentTask = currentTask;
            if (currentTask instanceof ConnectionTask) {
                ((ConnectionTask) currentTask).addSsl(this::addSslHandler);
            }
            this.taskStack = TaskStack.START;
            Publisher<ByteBuf> publisher;
            publisher = currentTask.startTask(this.taskSignal);
            this.taskStack = null;
            if (publisher == null) {
                this.upstream.request(128L);
                drainToTask();
            } else {
                // send packet
                sendPacket(currentTask, publisher)
                        .subscribe();
            }
        }


    }

    /**
     * @see #startHeadIfNeed()
     * @see #drainToTask()
     */
    private void disconnection() {
        if (this.currentTask instanceof ConnectionTask) {
            Logger LOG = obtainLogger();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Task[{}] disconnect.", this.currentTask);
            }
            this.connection.channel()
                    .close();
        } else {
            throw new IllegalStateException(String.format("Current %s[%s] isn't %s,reject disconnect."
                    , CommunicationTask.class.getName(), this.currentTask, ConnectionTask.class.getName()));
        }
    }

    /**
     * @see ConnectionTask#addSsl(Consumer)
     */
    private void addSslHandler(final SslWrapper sslWrapper) {
        final ConnectionTask currentTask = sslWrapper.getCurrentTask();
        if (currentTask != this.currentTask) {
            return;
        }
        final Publisher<ByteBuf> publisher = sslWrapper.getPublisher();
        if (publisher != null) {
            Mono.from(this.connection.outbound().send(publisher))
                    .doOnError(e -> disconnection())
                    .doOnSuccess(v -> {
                        if (this.eventLoop.inEventLoop()) {
                            doAddSslHandler(sslWrapper.getSslObject());
                        } else {
                            this.eventLoop.execute(() -> doAddSslHandler(sslWrapper.getSslObject()));
                        }
                    })
                    .subscribe();
        } else if (this.eventLoop.inEventLoop()) {
            doAddSslHandler(sslWrapper.getSslObject());
        } else {
            this.eventLoop.execute(() -> doAddSslHandler(sslWrapper.getSslObject()));
        }
    }

    /**
     * @see #addSslHandler(SslWrapper)
     */
    private void doAddSslHandler(final Object sslObject) {

        final Logger LOG = obtainLogger();
        final boolean traceEnabled = LOG.isTraceEnabled();
        final ChannelPipeline pipeline = this.connection.channel().pipeline();

        addSslHandshakeSuccessListener(pipeline, LOG);

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
        } else {
            throw new IllegalArgumentException(String.format("Not support %s type.", sslObject.getClass().getName()));
        }
        this.upstream.request(512L);
    }

    /**
     * @see #doAddSslHandler(Object)
     */
    private void addSslHandshakeSuccessListener(ChannelPipeline pipeline, Logger LOG) {
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
     * @see #addSslHandshakeSuccessListener(ChannelPipeline, Logger)
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


    /**
     * @see CommunicationTask#sendPacketSignal(boolean)
     * @see TaskSingleImpl#sendPacket(CommunicationTask, boolean)
     */
    private void doSendPacketSignal(final MonoSink<Void> sink, final CommunicationTask signalTask, boolean endTask) {
        if (obtainLogger().isDebugEnabled()) {
            obtainLogger().debug("{} send packet signal", signalTask);
        }
        if (signalTask == this.currentTask) {
            Publisher<ByteBuf> publisher;
            publisher = signalTask.moreSendPacket();
            if (publisher != null) {
                sendPacket(signalTask, publisher);
            }
            sink.success();
            if (endTask) {
                this.currentTask = null;
                startHeadIfNeed();
            }
        } else {
            sink.error(new IllegalArgumentException(String.format("task[%s] isn't current task.", signalTask)));
        }
    }


    private Mono<Void> sendPacket(final CommunicationTask headTask, final Publisher<ByteBuf> packetPublisher) {
        return Mono.from(this.connection.outbound().send(packetPublisher))
                .doOnError(cause -> {
                    if (this.eventLoop.inEventLoop()) {
                        handleSendPacketError(headTask, cause);
                    } else {
                        this.eventLoop.execute(() -> handleSendPacketError(headTask, cause));
                    }
                })
                .doOnSuccess(v -> this.upstream.request(128L));
    }

    /**
     * @see #sendPacket(CommunicationTask, Publisher)
     */
    private void handleSendPacketError(final CommunicationTask task, final Throwable cause) {
        Logger logger = obtainLogger();
        if (logger.isDebugEnabled()) {
            logger.error("CommunicationTask:{}", task, cause);
        }
        final CommunicationTask.Action action;
        action = task.errorEvent(cause);

        if (action == CommunicationTask.Action.MORE_SEND_AND_END) {
            final Publisher<ByteBuf> publisher = task.moreSendPacket();
            if (publisher != null) {
                sendPacket(task, publisher)
                        .subscribe();
            }
        }

        if (this.currentTask == task) {
            this.currentTask = null;
            if (task instanceof ConnectionTask && ((ConnectionTask) task).disconnect()) {
                disconnection();
            } else {
                startHeadIfNeed();
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
        public void syncSubmitTask(CommunicationTask task, Consumer<Void> errorConsumer) {
            this.taskExecutor.syncPushTask(task, errorConsumer);
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


    protected static final class TaskExecutorException extends IllegalStateException {

        public TaskExecutorException(String s) {
            super(s);
        }

        public TaskExecutorException(String message, Throwable cause) {
            super(message, cause);
        }

    }

    private static final class TaskSingleImpl implements TaskSignal {

        private final CommunicationTaskExecutor<?> taskExecutor;

        private TaskSingleImpl(CommunicationTaskExecutor<?> taskExecutor) {
            this.taskExecutor = taskExecutor;
        }


        @Override
        public Mono<Void> sendPacket(final CommunicationTask task, final boolean endTask) {
            return Mono.create(sink -> {
                if (this.taskExecutor.eventLoop.inEventLoop()) {
                    this.taskExecutor.doSendPacketSignal(sink, task, endTask);
                } else {
                    this.taskExecutor.eventLoop.execute(()
                            -> this.taskExecutor.doSendPacketSignal(sink, task, endTask));
                }
            });
        }

    }


    private enum TaskStack {
        START,
        DECODE,
        ERROR
    }


}
