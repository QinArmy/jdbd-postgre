package io.jdbd.vendor.result;

import io.jdbd.ResultStatusConsumerException;
import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.ResultType;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.function.Consumer;

/**
 * @see FluxResult
 */
final class QueryResultSubscriber extends AbstractResultSubscriber<Result> {

    static Flux<ResultRow> create(Consumer<ResultState> stateConsumer
            , Consumer<FluxResultSink> callback) {
        final FluxResult result = FluxResult.create(sink -> {
            try {
                callback.accept(sink);
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Flux.create(sink -> result.subscribe(new QueryResultSubscriber(sink, stateConsumer)));
    }


    private final FluxSink<ResultRow> sink;

    private final Consumer<ResultState> stateConsumer;

    private ResultState state;

    private QueryResultSubscriber(FluxSink<ResultRow> sink, Consumer<ResultState> stateConsumer) {
        this.sink = sink;
        this.stateConsumer = stateConsumer;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(final Result result) {
        // this method invoker in EventLoop
        if (hasError()) {
            return;
        }

        if (result.getResultIndex() != 0) {
            addSubscribeError(ResultType.MULTI_RESULT);
        } else if (result instanceof ResultRow) {
            this.sink.next((ResultRow) result);
        } else if (result instanceof ResultState) {
            final ResultState state = (ResultState) result;
            if (!state.hasReturningColumn()) {
                addSubscribeError(ResultType.UPDATE);
            } else if (state.hasMoreFetch()) {
                try {
                    this.stateConsumer.accept(state);
                } catch (Throwable e) {
                    addError(ResultStatusConsumerException.create(this.stateConsumer, e));
                }
            } else if (this.state == null) {
                this.state = state;
            } else {
                throw createDuplicationResultState(state);
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

        if (errorList == null || errorList.isEmpty()) {
            final ResultState state = this.state;
            if (state == null) {
                this.sink.error(new NoMoreResultException("No receive terminator query ResultState from upstream."));
            } else if (fluxSinkComplete(this.sink, stateConsumer, state)) {
                final List<Throwable> newErrorList = this.errorList;
                assert newErrorList != null;
                this.sink.error(JdbdExceptions.createException(newErrorList));
            } else {
                this.sink.complete();
            }
        } else {
            this.sink.error(JdbdExceptions.createException(errorList));
        }
    }

    @Override
    final ResultType getSubscribeType() {
        return ResultType.QUERY;
    }


}
