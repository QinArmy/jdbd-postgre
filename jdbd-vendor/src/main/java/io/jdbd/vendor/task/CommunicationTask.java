package io.jdbd.vendor.task;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * @see CommunicationTaskExecutor
 */
public abstract class CommunicationTask {

    final ITaskAdjutant adjutant;

    protected Publisher<ByteBuf> packetPublisher;

    private TaskPhase taskPhase;

    private TaskSignal taskSignal;

    private MethodStack methodStack;

    protected CommunicationTask(ITaskAdjutant adjutant) {
        this.adjutant = adjutant;
    }

    /**
     * <p>
     * {@link CommunicationTaskExecutor} invoke this method start task.
     * </p>
     *
     * @return <ul>
     * <li>if non-null {@link CommunicationTaskExecutor} will send this {@link Publisher}.</li>
     * <li>if null {@link CommunicationTaskExecutor} immediately invoke {@link #decodePackets(ByteBuf, Consumer)}</li>
     * </ul>
     */
    @Nullable
    final Publisher<ByteBuf> startTask(TaskSignal signal) {
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw createTaskPhaseException(TaskPhase.SUBMITTED);
        }
        this.taskSignal = Objects.requireNonNull(signal, "signal");
        this.taskPhase = TaskPhase.STARTED;
        this.methodStack = MethodStack.START;

        Publisher<ByteBuf> publisher;
        publisher = start();

        this.methodStack = null;
        return publisher;
    }


    /**
     * <p>
     * {@link CommunicationTaskExecutor} invoke this method read response from database server.
     * </p>
     *
     * @return true : task end.
     */
    final boolean decodePackets(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer)
            throws TaskStatusException {
        if (this.taskPhase != TaskPhase.STARTED) {
            throw createTaskPhaseException(TaskPhase.STARTED);
        }
        if (!hasOnePacket(cumulateBuffer)) {
            return false;
        }
        this.methodStack = MethodStack.DECODE;
        try {

            boolean taskEnd;
            taskEnd = decode(cumulateBuffer, serverStatusConsumer);
            if (taskEnd) {
                this.taskPhase = TaskPhase.END;
            }
            return taskEnd;
        } catch (Throwable e) {
            throw new TaskStatusException(e, "decode(ByteBuf, Consumer<Object>) method throw exception.");
        } finally {
            this.methodStack = null;
        }

    }

    /**
     * <p>
     * {@link CommunicationTaskExecutor} invoke this method get more send packet.
     * </p>
     *
     * @return if non-null {@link CommunicationTaskExecutor} will send this {@link Publisher}.
     */
    @Nullable
    final Publisher<ByteBuf> moreSendPacket() {
        final Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher != null) {
            this.packetPublisher = null;
        }
        return publisher;
    }


    /**
     * <p>
     * when network channel close,{@link CommunicationTaskExecutor} invoke this method.
     * </p>
     */
    final void channelCloseEvent() {
        this.taskPhase = TaskPhase.END;
        onChannelClose();
    }


    @Nullable
    final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }


    /**
     * <p>
     * sub class invoke this method submit task to {@link CommunicationTaskExecutor}
     * </p>
     */
    protected final void submit(Consumer<Throwable> consumer) {
        if (this.adjutant.inEventLoop()) {
            syncSubmitTask(consumer);
        } else {
            this.adjutant.execute(() -> syncSubmitTask(consumer));
        }
    }

    /**
     * <p>
     * {@link CommunicationTaskExecutor} invoke this method handle error,when below situation:
     *     <ul>
     *         <li>occur network error: ignore return {@link Action}</li>
     *         <li>packet send failure: handle return {@link Action}</li>
     *         <li>
     *             <ol>
     *                  <li>{@link #decodePackets(ByteBuf, Consumer)} throw {@link TaskStatusException}:
     *                  ignore return {@link Action} and invoke {@link #moreSendPacket()}
     *                  after {@link CommunicationTaskExecutor#clearChannel} return true.</li>
     *                  <li>{@link #decodePackets(ByteBuf, Consumer)} return true, but cumulateBuffer {@link ByteBuf#isReadable()}:ignore return {@link Action}</li>
     *             </ol>
     *         </li>
     *     </ul>
     * </p>
     *
     * @return <ul>
     *     <li>{@link Action#MORE_SEND_AND_END} :task end after {@link CommunicationTaskExecutor} invoke {@link #moreSendPacket()} and sent</li>
     *      <li>{@link Action#TASK_END} :task immediately end </li>
     * </ul>
     */
    final Action errorEvent(Throwable e) {
        if (this.taskPhase == TaskPhase.END) {
            return Action.TASK_END;
        }
        this.taskPhase = TaskPhase.END;
        this.methodStack = MethodStack.ERROR;

        Action action;
        action = onError(e);

        this.methodStack = null;

        return action;
    }

    /**
     * <p>
     * this method is useful,when receive params from application(or persistent framework) developer
     * and  execute statement after prepare statement.
     * </p>
     *
     * @param endTask true : task end after invoke {@link #moreSendPacket()}.
     * @return <ul>
     * <li>emit success when {@link CommunicationTaskExecutor} accept signal</li>
     * <li>emit {@link Throwable} when {@link CommunicationTaskExecutor} reject signal</li>
     * </ul>
     */
    protected final Mono<Void> sendPacketSignal(boolean endTask) {
        final Mono<Void> mono;
        if (this.adjutant.inEventLoop()) {
            if (this.methodStack == MethodStack.DECODE) {
                mono = Mono.empty();
            } else if (this.methodStack != null) {
                mono = Mono.error(new IllegalStateException
                        (String.format("Unsupported invoke in %s method stack.", this.methodStack)));
            } else {
                mono = this.taskSignal.sendPacket(this, endTask);
            }
        } else {
            mono = this.taskSignal.sendPacket(this, endTask);
        }
        return mono;
    }


    /**
     * @see #channelCloseEvent()
     */
    protected void onChannelClose() {

    }

    /**
     * <p>
     * this method shouldn't throw {@link Throwable} ,because that will cause network channel close.
     * </p>
     */
    protected abstract Action onError(Throwable e);


    /**
     * <p>
     * this method shouldn't throw {@link Throwable} ,because that will cause network channel close.
     * </p>
     */
    @Nullable
    protected abstract Publisher<ByteBuf> start();


    /**
     * <p>
     * this method shouldn't throw {@link Throwable} ,that mean bug.
     * But if this method throw {@link Throwable} ,{@link CommunicationTaskExecutor} will invoke
     * {@link CommunicationTaskExecutor#clearChannel(ByteBuf, Class)} clear network channel,
     *
     * </p>
     *
     * @return true ,task end.
     */
    protected abstract boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);


    protected abstract boolean hasOnePacket(ByteBuf cumulateBuffer);



    /*################################## blow private method ##################################*/

    /**
     * @see #submit(Consumer)
     */
    private void syncSubmitTask(final Consumer<Throwable> consumer) {
        if (this.taskPhase == null) {
            try {
                this.adjutant.syncSubmitTask(this, this::updateSubmitResult);
            } catch (Throwable e) {
                consumer.accept(e);
            }
        } else {
            consumer.accept(new IllegalStateException("Communication task have submitted."));
        }
    }


    private void updateSubmitResult(Void v) {
        if (this.taskPhase != null) {
            throw new IllegalStateException(String.format("this.taskPhase[%s] isn't null", this.taskPhase));
        }
        this.taskPhase = TaskPhase.SUBMITTED;
    }


    private IllegalStateException createTaskPhaseException(TaskPhase expect) {
        String message = String.format("%s TaskPhase[%s] isn't %s", this, this.taskPhase, expect);
        return new IllegalStateException(message);
    }


    protected enum Action {
        MORE_SEND_AND_END,
        TASK_END
    }


    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }

    private enum MethodStack {
        START,
        DECODE,
        ERROR,
    }


}
