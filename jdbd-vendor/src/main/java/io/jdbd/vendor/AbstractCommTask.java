package io.jdbd.vendor;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.lang.Nullable;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public abstract class AbstractCommTask implements CommTask<ByteBuf>, ReactorMultiResults {

    private final CommTaskExecutorAdjutant executorAdjutant;

    protected final int expectedResultCount;

    private int receiveResultCount;

    private TaskPhase taskPhase;


    protected AbstractCommTask(CommTaskExecutorAdjutant executorAdjutant, int expectedResultCount) {
        this.executorAdjutant = executorAdjutant;
        this.expectedResultCount = expectedResultCount;
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
    public final Mono<Long> nextUpdate(Consumer<ResultStates> statesConsumer) {
        return Mono.empty();
    }

    @Override
    public final <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        return Flux.empty();
    }

    protected final boolean skipCurrentResultRestRows() {
        return false;
    }

    protected final void emitErrorPacket(Throwable e) {

    }

    protected final void emitUpdateResult(ReactorMultiResults resultStates) {
        this.receiveResultCount++;
    }

    protected final void emitRowTerminator(ReactorMultiResults resultStates) {
        this.receiveResultCount++;
    }

    protected final FluxSink<ResultRow> obtainQueryResultSink() {
        return null;
    }


    @Nullable
    protected abstract ByteBuf internalStart();

    protected abstract boolean internalDecode(ByteBuf cumulateBuffer);


}
