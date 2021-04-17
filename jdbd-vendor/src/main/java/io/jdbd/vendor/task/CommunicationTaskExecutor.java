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
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.function.Consumer;

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

    private final TaskSignal taskSignal;

    private Subscription upstream;

    private CommunicationTask currentTask;

    private TaskStatusException taskErrorMethodError;

    private int packetIndex = -1;


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
            currentTask = this.taskQueue.poll();
            this.currentTask = currentTask;
        }
        if (currentTask == null) {
            return;
        }
        //store packet start index.
        this.packetIndex = cumulateBuffer.readerIndex();


        try {
            boolean taskEnd;

            try {
                taskEnd = currentTask.decode(cumulateBuffer, this::updateServerStatus);
            } catch (TaskStatusException e) {
                throw e;
            } catch (Throwable e) {
                String message = String.format("%s[%s] decode(ByteBuf,Consumer) method throw exception."
                        , CommunicationTask.class.getSimpleName(), currentTask);
                throw new TaskStatusException(e, message);
            }

            if (taskEnd && currentTask instanceof ConnectionTask) {
                ConnectionTask connectionTask = (ConnectionTask) currentTask;
                if (connectionTask.disconnect()) {
                    disconnection();
                    return;
                }
            }

            Publisher<ByteBuf> bufPublisher = null;

            try {
                bufPublisher = currentTask.moreSendPacket();
                if (bufPublisher != null) {
                    // send packet
                    sendPacket(currentTask, bufPublisher);
                }
            } catch (TaskStatusException e) {
                throw e;
            } catch (Throwable e) {
                String message = String.format("%s[%s] moreSendPacket() method throw exception."
                        , CommunicationTask.class.getSimpleName(), currentTask);
                throw new TaskStatusException(e, message);
            }

            if (taskEnd) {
                if (cumulateBuffer.isReadable()) {
                    // TODO maybe has notify from server.
                    try {
                        currentTask.error(new TaskStatusException("Not read all packet,but task end."));
                    } catch (TaskStatusException e) {
                        this.taskErrorMethodError = e;
                    } catch (Throwable e) {
                        this.taskErrorMethodError = new TaskStatusException(e, "error(Throwable) throw exception.");
                    }
                }
                this.currentTask = null; // current task end.
                // start next task
                startHeadIfNeed();
            }
        } catch (TaskStatusException e) {
            this.taskErrorMethodError = e;
        } catch (Throwable e) {
            // here executor has bug.
            String message = String.format("%s executor throw error,", CommunicationTask.class.getSimpleName());
            throw new TaskStatusException(message, e);
        }

    }


    protected abstract void updateServerStatus(Object serverStatus);

    protected abstract T createTaskAdjutant();

    protected abstract HostInfo<?> obtainHostInfo();

    /**
     * @return true : clear channel complement
     */
    protected abstract boolean clearChannel(ByteBuf cumulateBuffer, int packetIndex);


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
            startHeadIfNeed();
        } else {
            offerCall.accept(Boolean.FALSE);
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

        if (!cumulateBuffer.isReadable()) {
            cumulateBuffer.release();
            if (this.cumulateBuffer == cumulateBuffer) {
                this.cumulateBuffer = null;
            }
        }


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
        final Logger LOG = obtainLogger();
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
     */
    private void startHeadIfNeed() {
        CommunicationTask currentTask = this.currentTask;
        if (currentTask != null) {
            return;
        }

        TaskStatusException taskErrorMethodError = this.taskErrorMethodError;

        while ((currentTask = this.taskQueue.poll()) != null) {

            if (currentTask.getTaskPhase() != CommunicationTask.TaskPhase.SUBMITTED) {
                currentTask.error(new TaskStatusException("CommunicationTask[%s] phase isn't SUBMITTED,cannot start."
                        , currentTask));
                continue;
            }
            if (taskErrorMethodError != null) {
                currentTask.error(taskErrorMethodError);
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
                    return;
                }
                connectionTask.addSsl(this::addSslHandler);
            }
            Publisher<ByteBuf> bufPublisher;
            try {
                // invoke task start() method.
                bufPublisher = currentTask.start(this.taskSignal);
            } catch (TaskStatusException e) {
                handleTaskError(currentTask, e);
                continue;
            } catch (Throwable e) {
                handleTaskError(currentTask, new TaskStatusException(e, "%s start(morePacketSignal) failure."
                        , currentTask));
                continue;
            }
            if (bufPublisher != null) {
                // send packet
                sendPacket(currentTask, bufPublisher);
            } else {
                this.upstream.request(128L);
                drainToTask();
            }
            break;
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
                // here task has bug.
                this.currentTask = null;
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

    private void doSendEndPacketSignal(final MonoSink<Void> sink, final CommunicationTask signalTask) {
        doSendPacketSignal(sink, signalTask);
        if (this.currentTask == signalTask) {
            this.currentTask = null;
            this.eventLoop.execute(this::startHeadIfNeed);
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
                .subscribe(v -> this.upstream.request(128L));
    }

    /**
     * @see #sendPacket(CommunicationTask, Publisher)
     * @see #startHeadIfNeed()
     * @see #doSendPacketSignal(MonoSink, CommunicationTask)
     */
    private void handleTaskError(final CommunicationTask task, final Throwable e) {
        try {
            final CommunicationTask.Action action;
            action = task.error(e);
            switch (action) {
                case TASK_END:
                    this.currentTask = null;
                    startHeadIfNeed();
                    break;
                case MORE_SEND_PACKET:
                    break;
                default:
                    throw JdbdExceptions.createUnknownEnumException(action);
            }
            currentTaskEndIfNeed(task);
        } catch (TaskStatusException te) {
            this.taskErrorMethodError = te;
        } catch (Throwable te) {
            this.taskErrorMethodError = new TaskStatusException(e, "%s error(Throwable) method throw error.", task);
        }

    }


    private void currentTaskEndIfNeed(final CommunicationTask task) {
        if (task == this.currentTask) {

            final ByteBuf cumulate = this.cumulateBuffer;
            if (cumulate != null) {
                // TODO sub class handle clear channel
                if (cumulate.isReadable()) {
                    cumulate.readerIndex(cumulate.writerIndex());
                }
                cumulate.release();
                this.cumulateBuffer = null;
            }

            this.currentTask = null;
            startHeadIfNeed();

        }


    }

    /**
     * @see CommunicationTask#start(TaskSignal)
     */
    @Nullable
    private Publisher<ByteBuf> invokeTaskStart(CommunicationTask task) throws TaskStatusException {
        try {
            return task.start(this.taskSignal);
        } catch (TaskStatusException e) {
            throw e;
        } catch (Throwable e) {
            String message = String.format("%s start(%s) method throw exception."
                    , CommunicationTask.class.getSimpleName(), TaskSignal.class.getSimpleName());
            throw new TaskStatusException(e, message);
        }

    }

    /**
     * @return true: task end,see {@link CommunicationTask#decode(ByteBuf, Consumer)}
     * @see CommunicationTask#decode(ByteBuf, Consumer)
     */
    private boolean invokeDecode(ByteBuf cumulateBuffer, CommunicationTask task) throws TaskStatusException {
        try {
            this.packetIndex = cumulateBuffer.readerIndex();
            return task.decode(cumulateBuffer, this::updateServerStatus);
        } catch (TaskStatusException e) {
            throw e;
        } catch (Throwable e) {
            String message = String.format("%s decode(%s,%s) method throw exception."
                    , CommunicationTask.class.getSimpleName()
                    , ByteBuf.class.getSimpleName()
                    , Consumer.class.getSimpleName());
            throw new TaskStatusException(e, message);
        }

    }


    /**
     * @see CommunicationTask#moreSendPacket()
     */
    private void invokeMoreSendPacket(CommunicationTask task) throws TaskStatusException {
        Publisher<ByteBuf> publisher;
        try {
            publisher = task.moreSendPacket();
        } catch (TaskStatusException e) {
            throw e;
        } catch (Throwable e) {
            String message = String.format("%s moreSendPacket() method throw exception."
                    , CommunicationTask.class.getSimpleName());
            throw new TaskStatusException(e, message);
        }

        if (publisher != null) {
            sendPacket(task, publisher);
        }

    }

    /**
     * @see CommunicationTask#getTaskPhase()
     */
    @Nullable
    private CommunicationTask.TaskPhase invokeGetTaskPhase(CommunicationTask task) throws TaskStatusException {
        try {
            return task.getTaskPhase();
        } catch (TaskStatusException e) {
            throw e;
        } catch (Throwable e) {
            String message = String.format("%s getTaskPhase() method throw exception."
                    , CommunicationTask.class.getSimpleName());
            throw new TaskStatusException(e, message);
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
        public Mono<Void> sendPacket(final CommunicationTask task) {
            return Mono.create(sink -> {
                if (this.taskExecutor.eventLoop.inEventLoop()) {
                    this.taskExecutor.doSendPacketSignal(sink, task);
                } else {
                    this.taskExecutor.eventLoop.execute(() -> this.taskExecutor.doSendPacketSignal(sink, task));
                }
            });
        }

        @Override
        public Mono<Void> sendEndPacket(CommunicationTask task) {
            return Mono.create(sink -> {
                if (this.taskExecutor.eventLoop.inEventLoop()) {
                    this.taskExecutor.doSendEndPacketSignal(sink, task);
                } else {
                    this.taskExecutor.eventLoop.execute(() -> this.taskExecutor.doSendEndPacketSignal(sink, task));
                }
            });

        }
    }


}
