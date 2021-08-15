package io.jdbd.vendor.result;

import io.jdbd.result.NoMoreResultException;
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

/**
 * @see FluxResult
 */
final class BatchUpdateResultSubscriber extends AbstractResultSubscriber<Result> {

    static Flux<ResultState> create(ITaskAdjutant adjutant, Consumer<FluxResultSink> callback) {
        final FluxResult result = FluxResult.create(adjutant, sink -> {
            try {
                callback.accept(sink);
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Flux.create(sink -> result.subscribe(new BatchUpdateResultSubscriber(sink)));
    }

    private final FluxSink<ResultState> sink;

    private boolean receiveResult;

    private BatchUpdateResultSubscriber(FluxSink<ResultState> sink) {
        this.sink = sink;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(final Result result) {
        // this method invoker in EventLoop
        if (hasError()) {
            return;
        }
        if (result instanceof ResultRow) {
            addSubscribeError(ResultType.QUERY);
        } else if (result instanceof ResultState) {
            final ResultState state = (ResultState) result;
            if (state.hasReturnColumn()) {
                addSubscribeError(ResultType.QUERY);
            } else {
                if (!this.receiveResult) {
                    this.receiveResult = true;
                }
                this.sink.next(state);
            }
        } else {
            throw createUnknownTypeError(result);
        }
    }

    @Override
    public final void onError(Throwable t) {
        // this method invoker in EventLoop
        this.sink.error(t);
    }

    @Override
    public final void onComplete() {
        // this method invoker in EventLoop
        final List<Throwable> errorList = this.errorList;
        if (errorList != null && !errorList.isEmpty()) {
            this.sink.error(JdbdExceptions.createException(errorList));
        } else if (this.receiveResult) {
            this.sink.complete();
        } else {
            this.sink.error(new NoMoreResultException("No receive any result from upstream."));
        }

    }

    @Override
    final ResultType getSubscribeType() {
        return ResultType.BATCH_UPDATE;
    }


}
