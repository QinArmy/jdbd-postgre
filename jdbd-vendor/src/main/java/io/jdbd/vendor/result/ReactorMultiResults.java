package io.jdbd.vendor.result;

import io.jdbd.SessionCloseException;
import io.jdbd.result.*;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.task.TaskAdjutant;
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

abstract class ReactorMultiResults {

    static ReactorMultiResult create(TaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
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

        private final TaskAdjutant adjutant;

        private volatile boolean done;

        private Throwable upstreamError;

        private Throwable downstreamError;

        private Subscription s;

        private SingleResultFluxSubscriber(Flux<SingleResult> upstream, TaskAdjutant adjutant) {
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
        public final void onError(Throwable t) {

            if (this.upstreamError == null) {
                drain(); // first drain ,second store upstreamError
                this.upstreamError = t;
            } else if (LOG.isDebugEnabled()) {
                LOG.error("upstream duplicate invoke error(Throwable) method.", this.upstreamError);
            }


        }

        @Override
        public final void onComplete() {
            this.done = true;
            drain();
        }


        private void subscribeUpstream() {
            this.upstream.subscribe(this);
        }

        @SuppressWarnings("unchecked")
        private void drain() {
            Object sinkObj;
            SingleResult result;


            Throwable downstreamError = this.downstreamError;
            final Throwable originalDownstreamError = downstreamError;
            final Throwable upstreamError = this.upstreamError;

            while (!this.sinkQueue.isEmpty() && !this.resultQueue.isEmpty()) {
                sinkObj = this.sinkQueue.poll();
                result = this.resultQueue.poll();

                assert sinkObj != null;
                assert result != null;

                if (upstreamError != null) {
                    if (sinkObj instanceof MonoSink) {
                        ((MonoSink<?>) sinkObj).error(upstreamError);
                    } else {
                        ((SinkPair) sinkObj).sink.error(upstreamError);
                    }
                    continue;
                } else if (downstreamError != null) {
                    if (sinkObj instanceof MonoSink) {
                        ((MonoSink<?>) sinkObj).error(downstreamError);
                    } else {
                        ((SinkPair) sinkObj).sink.error(downstreamError);
                    }
                    continue;
                }

                if (sinkObj instanceof MonoSink) {
                    if (result.isQuery()) {
                        String message = String.format("Subscribe Update[index(based 0):%s] ,but actual Query."
                                , result.getIndex());
                        downstreamError = new SubscribeException(ResultType.UPDATE, ResultType.QUERY, message);
                        this.downstreamError = downstreamError;
                        ((MonoSink<?>) sinkObj).error(downstreamError);
                        Flux.from(result.receiveQuery())
                                .subscribe(cancelSubscribe());
                    } else {
                        MonoSink<ResultStatus> sink = ((MonoSink<ResultStatus>) sinkObj);
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
                        downstreamError = new SubscribeException(ResultType.QUERY, ResultType.UPDATE, message);
                        this.downstreamError = downstreamError;
                        pair.sink.error(downstreamError);
                        Mono.from(result.receiveUpdate())
                                .subscribe();
                    }
                }


            }

            if (originalDownstreamError == null && downstreamError != null) {
                s.cancel();
            }

        }

        private void subscribeUpdate(MonoSink<ResultStatus> sink) {
            if (this.s == null) {
                subscribeUpstream();
            } else if (this.done && this.resultQueue.isEmpty()) {
                sink.error(new NoMoreResultException("No more result."));
            } else {
                this.sinkQueue.add(sink);
                drain();
            }

        }

        private void subscribeQuery(FluxSink<ResultRow> sink, Consumer<ResultStatus> statesConsumer) {
            if (this.s == null) {
                subscribeUpstream();
            } else if (this.done && this.resultQueue.isEmpty()) {
                sink.error(new NoMoreResultException("No more result."));
            } else {
                this.sinkQueue.add(new SinkPair(sink, statesConsumer));
                drain();
            }

        }

    } // SingleResultFluxSubscriber


    @SuppressWarnings("unchecked")
    static <T> Subscriber<T> cancelSubscribe() {
        return (Subscriber<T>) CancelSubscribe.INSTANCE;
    }


    private static final class ReactorMultiResultImpl implements ReactorMultiResult {

        private final SingleResultFluxSubscriber upstream;

        private ReactorMultiResultImpl(SingleResultFluxSubscriber upstream) {
            this.upstream = upstream;
        }

        @Override
        public final Mono<ResultStatus> nextUpdate() {
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
        public final Flux<ResultRow> nextQuery(Consumer<ResultStatus> statesConsumer) {
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

        private final Consumer<ResultStatus> statesConsumer;

        private SinkPair(FluxSink<ResultRow> sink, Consumer<ResultStatus> statesConsumer) {
            this.sink = sink;
            this.statesConsumer = statesConsumer;
        }

    }


}
