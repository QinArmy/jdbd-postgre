package io.jdbd.vendor;

import io.jdbd.TaskQueueOverflowException;
import io.jdbd.lang.Nullable;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

public abstract class AbstractCommunicationTask implements CommunicationTask<ByteBuf> {

    final CommTaskExecutorAdjutant executorAdjutant;

    private TaskPhase taskPhase;

    protected AbstractCommunicationTask(CommTaskExecutorAdjutant executorAdjutant) {
        this.executorAdjutant = executorAdjutant;
    }

    @Nullable
    @Override
    public final ByteBuf start() {
        if (!this.executorAdjutant.inEventLoop()) {
            throw new IllegalStateException("start() isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw new IllegalStateException("taskPhase not null");
        }
        ByteBuf byteBuf = internalStart();
        this.taskPhase = TaskPhase.STARTED;
        return byteBuf;
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer) {
        if (!this.executorAdjutant.inEventLoop()) {
            throw new IllegalStateException("decode(ByteBuf) isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.STARTED) {
            throw new IllegalStateException("Communication task not start.");
        }
        boolean taskEnd;
        taskEnd = internalDecode(cumulateBuffer);
        if (taskEnd) {
            this.taskPhase = TaskPhase.END;
        }
        return taskEnd;
    }

    @Override
    public final void error(Throwable e) {
        this.taskPhase = TaskPhase.END;
        internalError(e);
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

    protected final void syncSubmit(Consumer<TaskQueueOverflowException> consumer) {
        if (this.taskPhase != null) {
            throw new IllegalStateException("Communication task have submitted.");
        }
        if (this.executorAdjutant.syncSubmitTask(this)) {
            this.taskPhase = TaskPhase.SUBMITTED;
        } else {
            consumer.accept(new TaskQueueOverflowException("Communication task queue overflow,cant' execute task."));
        }

    }

    protected void internalError(Throwable e) {

    }

    protected void internalOnChannelClose() {

    }

    @Nullable
    protected abstract ByteBuf internalStart();

    protected abstract boolean internalDecode(ByteBuf cumulateBuffer);


    /*################################## blow package static method ##################################*/


}
