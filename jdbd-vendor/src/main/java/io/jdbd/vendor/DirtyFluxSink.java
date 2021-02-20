package io.jdbd.vendor;

import io.jdbd.ResultRow;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.util.context.Context;

import java.util.function.LongConsumer;

public final class DirtyFluxSink implements FluxSink<ResultRow> {

    public static final DirtyFluxSink INSTANCE = new DirtyFluxSink();


    private DirtyFluxSink() {
    }

    @Override
    public boolean isCancelled() {
        // always true
        return true;
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
