package io.jdbd.vendor.result;

import io.jdbd.SessionCloseException;
import io.jdbd.result.*;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;

@Deprecated
abstract class ReactorMultiResults {

    static ReactorMultiResult create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        final Flux<SingleResult> flux = Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return new ReactorMultiResultImpl(new SingleResultFluxSubscriber(flux, adjutant));
    }

    private static final Logger LOG = LoggerFactory.getLogger(ReactorMultiResults.class);

    ReactorMultiResults() {
        throw new UnsupportedOperationException();
    }


    @SuppressWarnings("all")
    private static final class SingleResultFluxSubscriber implements Subscriber<SingleResult> {

        private final Queue<SingleResult> resultQueue = new LinkedList<>();

        private final Queue<Object> sinkQueue = new LinkedList<>();

        private final Flux<SingleResult> upstream;

        private final ITaskAdjutant adjutant;

        private volatile boolean done;

        private ErrorPair errorPair;

        private Subscription s;

        private SingleResultFluxSubscriber(Flux<SingleResult> upstream, ITaskAdjutant adjutant) {
            this.upstream = upstream;
            this.adjutant = adjutant;
        }

        @Override
        public final void onSubscribe(Subscription s) {
            this.s = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public final void onNext(SingleResult singleResult) {
            this.resultQueue.add(singleResult);
            drain();
        }

        @Override
        public final void onError(final Throwable t) {
            if (this.errorPair == null) {
                drain(); // first drain ,second store upstreamError
                this.errorPair = new ErrorPair(t, true);
                doOnError();
            }

        }

        @Override
        public final void onComplete() {
            this.done = true;
            if (this.errorPair == null) {
                drain();
                final Queue<Object> sinkQueue = this.sinkQueue;
                if (sinkQueue.isEmpty()) {
                    return;
                }

                Object sinkObj;
                final Throwable error = new NoMoreResultException("No more result.");
                while ((sinkObj = sinkQueue.poll()) != null) {
                    if (sinkObj instanceof MonoSink) {
                        ((MonoSink<?>) sinkObj).error(error);
                    } else {
                        SinkPair sinkPair = (SinkPair) sinkObj;
                        sinkPair.sink.error(error);
                    }
                }

            }

        }


        private void subscribeUpstream() {
            this.upstream.subscribe(this);
        }

        private void doOnError() {
            final ErrorPair errorPair = this.errorPair;
            if (errorPair == null) {
                throw new IllegalStateException("No error,reject execute.");
            }

            final Queue<SingleResult> resultQueue = this.resultQueue;
            SingleResult result;
            while ((result = resultQueue.poll()) != null) {
                if (result.isQuery()) {
                    Flux.from(result.receiveQuery())
                            .ignoreElements()
                            .subscribe();
                } else {
                    Mono.from(result.receiveUpdate())
                            .ignoreElement()
                            .subscribe();
                }

            }

            final Queue<Object> sinkQueue = this.sinkQueue;
            Object sinkObj;

            while ((sinkObj = sinkQueue.poll()) != null) {
                if (sinkObj instanceof MonoSink) {
                    ((MonoSink<?>) sinkObj).error(errorPair.error);
                } else {
                    ((SinkPair) sinkObj).sink.error(errorPair.error);
                }
            }


        }

        @SuppressWarnings("unchecked")
        private void drain() {
            if (this.errorPair != null) {
                doOnError();
                return;
            }

            final Queue<Object> sinkQueue = this.sinkQueue;
            final Queue<SingleResult> resultQueue = this.resultQueue;

            Object sinkObj;
            SingleResult result;

            while (!sinkQueue.isEmpty() && !resultQueue.isEmpty()) {
                sinkObj = this.sinkQueue.poll();
                result = this.resultQueue.poll();

                assert sinkObj != null;
                assert result != null;

                if (sinkObj instanceof MonoSink) {
                    if (result.isQuery()) {
                        String message = String.format("Subscribe Update[index(based 0):%s] ,but actual Query."
                                , result.getIndex());
                        Throwable error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY, message);
                        this.errorPair = new ErrorPair(error, false);
                        ((MonoSink<?>) sinkObj).error(error);
                        Flux.from(result.receiveQuery())
                                .ignoreElements()
                                .subscribe();
                        break;
                    } else {
                        MonoSink<ResultStates> sink = ((MonoSink<ResultStates>) sinkObj);
                        Mono.from(result.receiveUpdate())
                                .doOnError(sink::error)
                                .doOnSuccess(sink::success)
                                .subscribe();
                    }
                } else {
                    final SinkPair pair = (SinkPair) sinkObj;

                    if (result.isQuery()) {
                        Flux.from(result.receiveQuery(pair.statesConsumer))
                                .doOnNext(pair.sink::next)
                                .doOnError(pair.sink::error)
                                .doOnComplete(pair.sink::complete)
                                .subscribe();
                    } else {
                        String message = String.format("Subscribe Query[index(based 0):%s] ,but actual Update."
                                , result.getIndex());
                        Throwable error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE, message);
                        this.errorPair = new ErrorPair(error, false);
                        pair.sink.error(error);
                        Mono.from(result.receiveUpdate())
                                .ignoreElement()
                                .subscribe();
                        break;
                    }
                }


            }

            if (this.errorPair != null) {
                s.cancel();
                doOnError();
            }

        }


        private void subscribeUpdate(MonoSink<ResultStates> sink) {
            if (this.done && this.resultQueue.isEmpty()) {
                sink.error(new NoMoreResultException("No more result."));
            } else {
                this.sinkQueue.add(sink);
                if (this.s == null) {
                    subscribeUpstream();
                } else if (this.errorPair != null) {
                    doOnError();
                } else {
                    drain();
                }
            }

        }


        private void subscribeQuery(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            if (this.done && this.resultQueue.isEmpty()) {
                sink.error(new NoMoreResultException("No more result."));
            } else {
                this.sinkQueue.add(new SinkPair(sink, statesConsumer));
                if (s == null) {
                    subscribeUpstream();
                } else if (this.errorPair != null) {
                    doOnError();
                } else {
                    drain();
                }
            }

        }

    } // SingleResultFluxSubscriber


    private static final class ReactorMultiResultImpl implements ReactorMultiResult {

        private final SingleResultFluxSubscriber upstream;

        private ReactorMultiResultImpl(SingleResultFluxSubscriber upstream) {
            this.upstream = upstream;
        }

        @Override
        public final Mono<ResultStates> nextUpdate() {
            return Mono.create(sink -> {
                if (this.upstream.adjutant.isActive()) {
                    if (this.upstream.adjutant.inEventLoop()) {
                        this.upstream.subscribeUpdate(sink);
                    } else {
                        this.upstream.adjutant.execute(() -> this.upstream.subscribeUpdate(sink));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        @Override
        public final Flux<ResultRow> nextQuery(Consumer<ResultStates> statesConsumer) {
            Objects.requireNonNull(statesConsumer, "statesConsumer");
            return Flux.create(sink -> {
                if (this.upstream.adjutant.isActive()) {
                    if (this.upstream.adjutant.inEventLoop()) {
                        this.upstream.subscribeQuery(sink, statesConsumer);
                    } else {
                        this.upstream.adjutant.execute(() -> this.upstream.subscribeQuery(sink, statesConsumer));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        @Override
        public final Flux<ResultRow> nextQuery() {
            return nextQuery(MultiResult.EMPTY_CONSUMER);
        }


    }


    @SuppressWarnings("all")
    private static final class CancelSubscribe<T> implements Subscriber<T> {

        private static final CancelSubscribe<?> INSTANCE = new CancelSubscribe<>();

        @Override
        public void onSubscribe(Subscription s) {
            s.cancel();
        }

        @Override
        public void onNext(T t) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onComplete() {

        }
    }

    private static final class SinkPair {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statesConsumer;

        private SinkPair(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            this.sink = sink;
            this.statesConsumer = statesConsumer;
        }

    }

    private static final class ErrorPair {

        private final Throwable error;

        private final boolean upstream;

        private ErrorPair(Throwable error, boolean upstream) {
            this.error = error;
            this.upstream = upstream;
        }

    }


}
