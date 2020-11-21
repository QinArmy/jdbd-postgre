package io.jdbd.vendor;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public abstract class AbstractSQLCommTask extends AbstractCommTask implements ReactorMultiResults {

    private final int expectedResultCount;

    private int receiveResultCount = 0;

    protected AbstractSQLCommTask(CommTaskExecutorAdjutant executorAdjutant, int expectedResultCount) {
        super(executorAdjutant);
        this.expectedResultCount = expectedResultCount;
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


    @Override
    public final void error(Throwable e) {

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


}
