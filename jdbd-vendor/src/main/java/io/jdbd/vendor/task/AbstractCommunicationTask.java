package io.jdbd.vendor.task;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.TaskQueueOverflowException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

public abstract class AbstractCommunicationTask implements CommunicationTask {

    final TaskAdjutant adjutant;

    private TaskPhase taskPhase;

    protected Publisher<ByteBuf> packetPublisher;

    protected AbstractCommunicationTask(TaskAdjutant adjutant) {
        this.adjutant = adjutant;
    }

    @Nullable
    @Override
    public final Publisher<ByteBuf> start(TaskSignal signal) {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("start() isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw new IllegalStateException("taskPhase not null");
        }
        Publisher<ByteBuf> publisher = internalStart(signal);
        this.taskPhase = TaskPhase.STARTED;
        return publisher;
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("decode(ByteBuf) isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.STARTED) {
            throw new IllegalStateException("Communication task not start.");
        }
        boolean taskEnd;
        taskEnd = internalDecode(cumulateBuffer, serverStatusConsumer);
        if (taskEnd) {
            this.taskPhase = TaskPhase.END;
        }
        return taskEnd;
    }

    @Nullable
    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher != null) {
            this.packetPublisher = null;
        }
        return publisher;
    }

    @Override
    public final boolean onSendSuccess() {
        boolean taskEnd;
        taskEnd = internalOnSendSuccess();
        if (taskEnd) {
            this.taskPhase = TaskPhase.END;
        }
        return taskEnd;
    }

    @Nullable
    @Override
    public final Publisher<ByteBuf> error(Throwable e) {
        this.taskPhase = TaskPhase.END;
        return internalError(e);
    }

    @Override
    public final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }

    @Override
    public final void onChannelClose() {
        this.taskPhase = TaskPhase.END;
        // TODO optimize
        internalOnChannelClose();

    }

    protected final void submit(Consumer<Throwable> consumer) {
        if (this.adjutant.inEventLoop()) {
            syncSubmitTask(consumer);
        } else {
            this.adjutant.execute(() -> syncSubmitTask(consumer));
        }
    }

    @Nullable
    protected Publisher<ByteBuf> internalError(Throwable e) {
        return null;
    }

    protected void internalOnChannelClose() {

    }

    protected boolean internalOnSendSuccess() {
        return false;
    }

    @Nullable
    protected abstract Publisher<ByteBuf> internalStart(TaskSignal signal);

    protected abstract boolean internalDecode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);


    /*################################## blow package static method ##################################*/

    /*################################## blow private method ##################################*/


    private void syncSubmitTask(Consumer<Throwable> consumer) {
        if (this.taskPhase != null) {
            throw new IllegalStateException("Communication task have submitted.");
        }
        try {
            this.adjutant.syncSubmitTask(this, success -> {
                if (success) {
                    this.taskPhase = TaskPhase.SUBMITTED;
                } else {
                    consumer.accept(new TaskQueueOverflowException("Communication task queue overflow,cant' execute task."));
                }
            });
        } catch (JdbdNonSQLException e) {
            consumer.accept(e);
        }

    }


}
