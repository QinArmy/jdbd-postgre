package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.ResultStatusConsumerException;
import io.jdbd.SessionCloseException;
import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.*;
import java.util.function.Consumer;

final class MultiResultSubscriber implements Subscriber<Result> {


    static ReactorMultiResult create(ITaskAdjutant adjutant, Consumer<FluxSink<Result>> callback) {
        final Flux<Result> flux = Flux.create(sink -> {
            try {
                callback.accept(sink);
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return new ReactorMultiResultImpl(adjutant, flux);
    }

    private final Flux<Result> source;

    private final Queue<Result> resultQueue = new LinkedList<>();

    private final Queue<SinkWrapper> sinkQueue = new LinkedList<>();

    private List<JdbdException> errorList;

    private Subscription subscription;

    private boolean done;


    private MultiResultSubscriber(Flux<Result> source) {
        this.source = source;
    }


    @Override
    public final void onSubscribe(Subscription s) {
        this.subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(final Result result) {
        // this method invoker in EventLoop
        if (!hasError()) {
            this.resultQueue.offer(result);
            drain();
        }
    }

    @Override
    public final void onError(Throwable t) {
        // this method invoker in EventLoop
        drain();
        addError(t);
        drainError();
    }

    @Override
    public final void onComplete() {
        // this method invoker in EventLoop
        this.done = true;
        if (hasError()) {
            drainError();
        } else {
            drain();
        }
    }


    private void fluxSinkComplete(FluxSink<ResultRow> sink, Consumer<ResultState> stateConsumer, ResultState state) {
        Throwable consumerError = null;
        try {
            stateConsumer.accept(state);
        } catch (Throwable e) {
            consumerError = e;
        }
        if (consumerError == null) {
            sink.complete();
        } else {
            sink.error(ResultStatusConsumerException.create(stateConsumer, consumerError));
        }
    }

    private void drainError() {
        final List<JdbdException> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            throw new IllegalStateException("No error");
        }
        final Queue<SinkWrapper> sinkQueue = this.sinkQueue;
        if (!sinkQueue.isEmpty()) {
            final JdbdException error = JdbdExceptions.createException(errorList);
            SinkWrapper sink;
            while ((sink = sinkQueue.poll()) != null) {
                sink.error(error);
            }
        }
        if (!this.resultQueue.isEmpty()) {
            this.resultQueue.clear();
        }
    }


    private void drain() {

        final Queue<SinkWrapper> sinkQueue = this.sinkQueue;
        final Queue<Result> resultQueue = this.resultQueue;

        SinkWrapper sink;
        Result currentResult;
        ResultType nonExpectedType = null;

        while ((sink = sinkQueue.peek()) != null) {
            currentResult = resultQueue.poll();
            if (currentResult == null) {
                break;
            }
            if (currentResult instanceof ResultRow) {
                final FluxSink<ResultRow> fluxSink = sink.fluxSink;
                if (fluxSink == null) {
                    nonExpectedType = ResultType.QUERY;
                    break;
                } else {
                    fluxSink.next((ResultRow) currentResult);
                }
            } else if (currentResult instanceof ResultState) {
                final ResultState state = (ResultState) currentResult;
                sinkQueue.poll();
                if (state.hasReturnColumn()) {
                    final FluxSink<ResultRow> fluxSink = sink.fluxSink;
                    if (fluxSink == null) {
                        nonExpectedType = ResultType.QUERY;
                        break;
                    } else {
                        final Consumer<ResultState> stateConsumer = sink.stateConsumer;
                        assert stateConsumer != null;
                        fluxSinkComplete(fluxSink, stateConsumer, state);
                    }
                } else {
                    final MonoSink<ResultState> monoSink = sink.updateSink;
                    if (monoSink == null) {
                        nonExpectedType = ResultType.UPDATE;
                        break;
                    } else {
                        monoSink.success(state);
                    }
                }
            } else {
                throw AbstractResultSubscriber.createUnknownTypeError(currentResult);
            }

        }

        if (nonExpectedType != null) {
            addSubscribeError(nonExpectedType);
            drainError();
        }

    }

    private void addSubscribeError(final ResultType nonExpectedType) {
        final List<JdbdException> errorList = this.errorList;
        if (errorList != null) {
            for (JdbdException e : errorList) {
                if (e instanceof SubscribeException) {
                    return;
                }
            }
        }
        switch (nonExpectedType) {
            case QUERY: {
                addError(new SubscribeException(ResultType.UPDATE, nonExpectedType));
            }
            break;
            case UPDATE: {
                addError(new SubscribeException(ResultType.QUERY, nonExpectedType));
            }
            break;
            default:
                throw new IllegalArgumentException(String.format("nonExpectedType[%s]", nonExpectedType));
        }

    }

    private boolean hasError() {
        List<JdbdException> errorList = this.errorList;
        return errorList != null && !errorList.isEmpty();
    }

    private void addError(Throwable t) {
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(JdbdExceptions.wrap(t));

    }


    private void subscribeInEventLoop(SinkWrapper sink) {
        if (hasError()) {
            this.sinkQueue.add(sink);
            drainError();
        } else if (this.done && this.resultQueue.isEmpty()) {
            sink.error(new NoMoreResultException("No more result."));
        } else {
            this.sinkQueue.add(sink);
            if (this.subscription == null) {
                // first subscribe
                this.source.subscribe(this);
            } else {
                drain();
            }

        }
    }


    private static final class ReactorMultiResultImpl implements ReactorMultiResult {

        private final ITaskAdjutant adjutant;

        private final MultiResultSubscriber subscriber;

        private ReactorMultiResultImpl(ITaskAdjutant adjutant, Flux<Result> source) {
            this.adjutant = adjutant;
            this.subscriber = new MultiResultSubscriber(source);
        }

        @Override
        public final Mono<ResultState> nextUpdate() {
            return Mono.create(sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        this.subscriber.subscribeInEventLoop(new SinkWrapper(sink));
                    } else {
                        this.adjutant.execute(() -> this.subscriber.subscribeInEventLoop(new SinkWrapper(sink)));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        @Override
        public final Flux<ResultRow> nextQuery(final Consumer<ResultState> statesConsumer) {
            return Flux.create(sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        this.subscriber.subscribeInEventLoop(new SinkWrapper(sink, statesConsumer));
                    } else {
                        this.adjutant.execute(() ->
                                this.subscriber.subscribeInEventLoop(new SinkWrapper(sink, statesConsumer)));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        @Override
        public final Flux<ResultRow> nextQuery() {
            return nextQuery(JdbdFunctions.noActionConsumer());
        }


    }


    private static final class SinkWrapper {

        private final FluxSink<ResultRow> fluxSink;

        private final Consumer<ResultState> stateConsumer;

        private final MonoSink<ResultState> updateSink;

        private SinkWrapper(FluxSink<ResultRow> fluxSink, Consumer<ResultState> stateConsumer) {
            this.fluxSink = fluxSink;
            this.stateConsumer = stateConsumer;
            this.updateSink = null;
        }

        private SinkWrapper(MonoSink<ResultState> updateSink) {
            this.updateSink = updateSink;
            this.fluxSink = null;
            this.stateConsumer = null;
        }

        void error(JdbdException error) {
            if (this.fluxSink == null) {
                Objects.requireNonNull(this.updateSink)
                        .error(error);
            } else {
                this.fluxSink.error(error);
            }
        }


    }


}
