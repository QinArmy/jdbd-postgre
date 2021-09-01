package io.jdbd.vendor.result;

import io.jdbd.ResultStatusConsumerException;
import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
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
final class QueryResultSubscriber extends AbstractResultSubscriber {

    static Flux<ResultRow> create(Consumer<ResultStates> stateConsumer
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

    private final Consumer<ResultStates> stateConsumer;

    private ResultStates state;

    private QueryResultSubscriber(FluxSink<ResultRow> sink, Consumer<ResultStates> stateConsumer) {
        this.sink = sink;
        this.stateConsumer = stateConsumer;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final boolean isCancelled() {
        return this.sink.isCancelled();
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
        } else if (result instanceof ResultStates) {
            final ResultStates state = (ResultStates) result;
            if (!state.hasColumn()) {
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
            final ResultStates state = this.state;
            if (state == null) {
                this.sink.error(new NoMoreResultException("No receive terminator query ResultState from upstream."));
            } else {
                fluxSinkComplete(this.sink, stateConsumer, state);
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
