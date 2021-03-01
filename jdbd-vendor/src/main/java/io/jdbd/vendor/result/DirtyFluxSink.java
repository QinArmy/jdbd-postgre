package io.jdbd.vendor.result;

import io.jdbd.ResultRow;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.util.context.Context;

import java.util.function.LongConsumer;

final class DirtyFluxSink implements FluxSink<ResultRow> {

    static final DirtyFluxSink INSTANCE = new DirtyFluxSink();


    private DirtyFluxSink() {
    }

    @Override
    public boolean isCancelled() {
        // always false
        return false;
    }

    @Override
    public FluxSink<ResultRow> next(ResultRow resultRow) {
        // no-op
        return this;
    }

    @Override
    public void complete() {
        // no-op
    }

    @Override
    public void error(Throwable e) {
        // no-op
    }

    @Override
    public Context currentContext() {
        return Context.empty();
    }

    @Override
    public long requestedFromDownstream() {
        return Long.MAX_VALUE;
    }

    @Override
    public FluxSink<ResultRow> onRequest(LongConsumer consumer) {
        // no-op
        return this;
    }

    @Override
    public FluxSink<ResultRow> onCancel(Disposable d) {
        // no-op
        return this;
    }

    @Override
    public FluxSink<ResultRow> onDispose(Disposable d) {
        // no-op
        return this;
    }
}
