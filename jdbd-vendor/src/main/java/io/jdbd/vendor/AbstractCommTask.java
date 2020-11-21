package io.jdbd.vendor;

import io.jdbd.lang.Nullable;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

public abstract class AbstractCommTask implements CommTask<ByteBuf> {

    final CommTaskExecutorAdjutant executorAdjutant;

    private TaskPhase taskPhase;

    protected AbstractCommTask(CommTaskExecutorAdjutant executorAdjutant) {
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
    public final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }

    @Override
    public void onChannelClose() {
        // TODO optimize
    }

    protected final Mono<Void> submit() {
        return this.executorAdjutant.submitTask(this)
                .doOnSuccess(v -> AbstractCommTask.this.taskPhase = TaskPhase.SUBMITTED)
                ;
    }

    @Nullable
    protected abstract ByteBuf internalStart();

    protected abstract boolean internalDecode(ByteBuf cumulateBuffer);


}
