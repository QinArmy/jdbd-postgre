package io.jdbd.vendor.result;

import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.ResultType;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.function.Consumer;

final class BatchUpdateResultSubscriber extends AbstractResultSubscriber<Result> {

    static Flux<ResultState> create(ITaskAdjutant adjutant, Consumer<FluxSink<Result>> callback) {
        final Flux<Result> flux = Flux.create(sink -> {
            try {
                callback.accept(sink);
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Flux.create(sink -> flux.subscribe(new BatchUpdateResultSubscriber(adjutant, sink)));
    }

    private final ITaskAdjutant adjutant;

    private final FluxSink<ResultState> sink;

    private BatchUpdateResultSubscriber(ITaskAdjutant adjutant, FluxSink<ResultState> sink) {
        super(adjutant);
        this.adjutant = adjutant;
        this.sink = sink;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(final Result result) {
        if (result instanceof ResultRow) {
            addError(ResultType.QUERY);
        } else if (result instanceof ResultState) {
            final ResultState state = (ResultState) result;
            if (state.hasReturnColumn()) {
                addError(ResultType.QUERY);
            } else if (this.adjutant.inEventLoop()) {
                if (!hasError()) {
                    this.sink.next(state);
                }
            } else {
                this.adjutant.execute(() -> {
                    if (!hasError()) {
                        this.sink.next(state);
                    }
                });
            }
        } else {
            throw createUnknownTypeError(result);
        }
    }

    @Override
    public final void onError(Throwable t) {
        this.sink.error(JdbdExceptions.wrap(t));
    }

    @Override
    public final void onComplete() {
        if (this.adjutant.inEventLoop()) {
            doCompleteInEventLoop();
        } else {
            this.adjutant.execute(this::doCompleteInEventLoop);
        }
    }

    @Override
    final ResultType getSubscribeType() {
        return ResultType.BATCH_UPDATE;
    }


    private void doCompleteInEventLoop() {
        final List<Throwable> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            this.sink.complete();
        } else {
            this.sink.error(JdbdExceptions.createException(errorList));
        }

    }


}
