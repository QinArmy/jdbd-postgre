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
            throw new IllegalStateException("start(MorePacketSignal) isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw new IllegalStateException("taskPhase not null");
        }
        try {
            Publisher<ByteBuf> publisher = internalStart(signal);
            this.taskPhase = TaskPhase.STARTED;
            return publisher;
        } catch (Throwable e) {
            throw new TaskStatusException(e, "start(MorePacketSignal) method throw exception.");
        }
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("decode(ByteBuf, Consumer<Object>) isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.STARTED) {
            throw new IllegalStateException("Communication task not start.");
        }
        try {
            boolean taskEnd;
            taskEnd = internalDecode(cumulateBuffer, serverStatusConsumer);
            if (taskEnd) {
                this.taskPhase = TaskPhase.END;
            }
            return taskEnd;
        } catch (Throwable e) {
            throw new TaskStatusException(e, "decode(ByteBuf, Consumer<Object>) method throw exception.");
        }
    }

    @Nullable
    @Override
    public final Publisher<ByteBuf> moreSendPacket() {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("moreSendPacket() isn't in EventLoop.");
        }
        Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher != null) {
            this.packetPublisher = null;
        }
        return publisher;
    }

    @Override
    public final Action error(Throwable e) {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("error(Throwable) isn't in EventLoop.");
        }
        this.taskPhase = TaskPhase.END;
        try {
            return internalError(e);
        } catch (Throwable t) {
            throw new TaskStatusException(t, "error(Throwable) method throw exception.");
        }

    }

    @Override
    public final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }

    @Override
    public final void onChannelClose() {
        if (!this.adjutant.inEventLoop()) {
            throw new IllegalStateException("onChannelClose() isn't in EventLoop.");
        }
        this.taskPhase = TaskPhase.END;
        // TODO optimize
        try {
            internalOnChannelClose();
        } catch (Throwable t) {
            throw new TaskStatusException(t, "onChannelClose() method throw exception.");
        }

    }

    protected final void submit(Consumer<Throwable> consumer) {
        if (this.adjutant.inEventLoop()) {
            syncSubmitTask(consumer);
        } else {
            this.adjutant.execute(() -> syncSubmitTask(consumer));
        }
    }

    protected abstract Action internalError(Throwable e);

    protected void internalOnChannelClose() {

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
