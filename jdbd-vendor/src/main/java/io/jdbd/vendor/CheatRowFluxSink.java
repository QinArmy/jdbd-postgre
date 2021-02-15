package io.jdbd.vendor;

import io.jdbd.ResultRow;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.util.context.Context;

import java.util.function.LongConsumer;

public final class CheatRowFluxSink implements FluxSink<ResultRow> {

    private static final CheatRowFluxSink INSTANCE = new CheatRowFluxSink();


    private CheatRowFluxSink() {
    }

    @Override
    public boolean isCancelled() {
        return true;
    }

    @Override
    public FluxSink<ResultRow> next(ResultRow resultRow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void complete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void error(Throwable e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Context currentContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long requestedFromDownstream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FluxSink<ResultRow> onRequest(LongConsumer consumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FluxSink<ResultRow> onCancel(Disposable d) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FluxSink<ResultRow> onDispose(Disposable d) {
        throw new UnsupportedOperationException();
    }
}
