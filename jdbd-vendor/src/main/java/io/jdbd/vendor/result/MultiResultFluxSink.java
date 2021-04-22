package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.ResultStatusConsumerException;
import io.jdbd.SessionCloseException;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.JdbdUnknownException;
import io.jdbd.vendor.task.TaskAdjutant;
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

final class MultiResultFluxSink implements MultiResultSink {

    static MultiResultSink create(FluxSink<SingleResult> sink, TaskAdjutant adjutant) {
        return new MultiResultFluxSink(sink, adjutant);
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiResultFluxSink.class);

    private final FluxSink<SingleResult> sink;

    private final TaskAdjutant adjutant;

    // non-volatile ,upstream in netty EventLoop
    private int index = 0;

    // non-volatile ,upstream in netty EventLoop
    private Throwable upstreamError;


    private MultiResultFluxSink(FluxSink<SingleResult> sink, TaskAdjutant adjutant) {
        this.sink = sink;
        this.adjutant = adjutant;
    }

    @Override
    public final boolean isCancelled() {
        return this.sink.isCancelled();
    }

    @Override
    public final void error(final Throwable e) {
        if (this.upstreamError == null) {
            this.upstreamError = e;
            this.sink.error(e);
        } else if (LOG.isDebugEnabled()) {
            // here dont' throw IllegalStateException ,avoid network channel has dirty byte.
            // jdbd api implementation should suite test to avoid bug.
            LOG.error("upstream duplicate invoke error(Throwable) method,first exception:\n", this.upstreamError);
        }
    }

    @Override
    public final void nextUpdate(ResultStatus status) {
        if (this.upstreamError == null) {
            final int currentIndex = this.index++;
            if (currentIndex < 0) {
                Throwable e = createIndexOverflowException(currentIndex);
                this.upstreamError = e;
                // here dont' throw IllegalStateException to upstream,avoid network channel has dirty byte.
                // jdbd api implementation should suite test to avoid bug.
                this.sink.error(e);
            } else if (!this.sink.isCancelled()) {
                this.sink.next(new UpdateSingleResult(currentIndex, status, this.adjutant));
            }

        }
    }

    @Override
    public final QuerySink nextQuery() {
        final QuerySink querySink;
        if (this.upstreamError == null) {
            final int currentIndex = this.index++;
            if (currentIndex < 0) {
                querySink = SkipQuerySink.INSTANCE;
                Throwable e = createIndexOverflowException(currentIndex);
                this.upstreamError = e;
                // here dont' throw IllegalStateException to upstream,avoid network has dirty byte.
                // jdbd api implementation should suite test to avoid bug.
                this.sink.error(e);
            } else if (this.sink.isCancelled()) {
                querySink = SkipQuerySink.INSTANCE;
            } else {
                final QuerySingleResult result = new QuerySingleResult(currentIndex, this.adjutant);
                querySink = result.querySink;
                this.sink.next(result);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.error("Upstream error nextQuery() method,after error.first exception:\n", this.upstreamError);
            }
            // here dont' throw IllegalStateException ,avoid network has dirty byte.
            // jdbd api implementation should suite test to avoid bug.
            querySink = SkipQuerySink.INSTANCE;
        }
        return querySink;
    }

    @Override
    public final void complete() {
        if (this.upstreamError == null) {
            this.sink.complete();
        }
    }

    private static JdbdException createIndexOverflowException(int index) {
        String message = String.format("Result index[%s] overflow Integer.MAX_VALUE[%s]", index, Integer.MAX_VALUE);
        IllegalStateException e = new IllegalStateException(message);
        return new JdbdUnknownException(message, e);
    }


    private static final class UpdateSingleResult implements ReactorSingleResult {

        private final int index;

        private final ResultStatus resultStatus;

        private final TaskAdjutant adjutant;

        private boolean done;

        private UpdateSingleResult(int index, ResultStatus resultStatus, TaskAdjutant adjutant) {
            this.index = index;
            this.resultStatus = resultStatus;
            this.adjutant = adjutant;
        }

        @Override
        public final boolean isQuery() {
            return false;
        }

        @Override
        public final int getIndex() {
            return this.index;
        }

        @Override
        public final Mono<ResultStatus> receiveUpdate() {
            return Mono.create(sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        downstreamSubscribe(sink);
                    } else {
                        this.adjutant.execute(() -> downstreamSubscribe(sink));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        private void downstreamSubscribe(MonoSink<ResultStatus> sink) {
            if (this.done) {
                String message = String.format("Update result[index:%s] can only subscribe one time.", this.index);
                sink.error(new IllegalStateException(message));
            } else {
                sink.success(this.resultStatus);
                this.done = true;
            }
        }


        @Override
        public final Flux<ResultRow> receiveQuery(Consumer<ResultStatus> statesConsumer) {
            return receiveQuery();
        }

        @Override
        public final Flux<ResultRow> receiveQuery() {
            String message = String.format("Subscribe query,but actual update result[index:%s]", this.index);
            return Flux.error(new SubscribeException(ResultType.QUERY, ResultType.UPDATE, message));
        }


    }


    private static final class QuerySingleResult implements ReactorSingleResult {

        private final int index;

        private final TaskAdjutant adjutant;

        private final QuerySinkImpl querySink;

        private QuerySingleResult(final int index, TaskAdjutant adjutant) {
            this.index = index;
            this.adjutant = adjutant;
            this.querySink = new QuerySinkImpl(this);
        }

        @Override
        public final boolean isQuery() {
            return true;
        }

        @Override
        public final int getIndex() {
            return this.index;
        }

        @Override
        public final Mono<ResultStatus> receiveUpdate() {
            String message = String.format("Subscribe update ,but actual Query result[index:%s]", this.index);
            return Mono.error(new SubscribeException(ResultType.UPDATE, ResultType.QUERY, message));
        }

        @Override
        public final Flux<ResultRow> receiveQuery(final Consumer<ResultStatus> statesConsumer) {
            Objects.requireNonNull(statesConsumer, "statesConsumer");

            return Flux.create(sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        this.querySink.downstreamSubscribe(sink, statesConsumer);
                    } else {
                        this.adjutant.execute(() -> this.querySink.downstreamSubscribe(sink, statesConsumer));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }

        @Override
        public final Flux<ResultRow> receiveQuery() {
            return receiveQuery(MultiResult.EMPTY_CONSUMER);
        }


    }

    private static final class QuerySinkImpl implements QuerySink {

        private final QuerySingleResult result;

        private Queue<ResultRow> queue;

        private boolean done;

        private ResultStatus resultStatus;

        private FluxSink<ResultRow> downstreamSink;

        private Consumer<ResultStatus> statusConsumer;

        private QuerySinkImpl(QuerySingleResult result) {
            this.result = result;
        }

        @Override
        public final void accept(ResultStatus resultStatus) {
            this.resultStatus = resultStatus;
        }

        @Override
        public final void complete() {
            this.done = true;
            doComplete();
        }

        @Override
        public final void next(final ResultRow resultRow) {
            final FluxSink<ResultRow> sink = this.downstreamSink;
            Queue<ResultRow> queue = this.queue;
            if (sink == null) {
                if (queue == null) {
                    queue = new LinkedList<>();
                    this.queue = queue;
                }
                queue.add(resultRow);
            } else {
                if (queue != null) {
                    drain();
                }
                sink.next(resultRow);
            }
        }

        @Override
        public final boolean isCancelled() {
            return this.downstreamSink != null && this.downstreamSink.isCancelled();
        }

        private void downstreamSubscribe(final FluxSink<ResultRow> sink, final Consumer<ResultStatus> statesConsumer) {
            if (this.downstreamSink == null) {
                this.downstreamSink = sink;
                this.statusConsumer = statesConsumer;
                if (this.done) {
                    doComplete();
                } else {
                    drain();
                }
            } else {
                sink.error(createDuplicateSubscribe(this.result.index));
            }

        }


        private void drain() {
            final FluxSink<ResultRow> sink = this.downstreamSink;
            if (sink == null) {
                return;
            }
            final Queue<ResultRow> queue = this.queue;
            if (queue != null) {
                ResultRow row;
                while ((row = queue.poll()) != null) {
                    sink.next(row);
                }
                this.queue = null;
            }

        }

        private void doComplete() {
            final FluxSink<ResultRow> sink = this.downstreamSink;
            if (sink == null) {
                return;
            }

            drain();

            final Consumer<ResultStatus> statusConsumer = this.statusConsumer;
            if (statusConsumer == null) {
                sink.complete();
            } else {
                try {
                    statusConsumer.accept(Objects.requireNonNull(this.resultStatus, "this.resultStatus"));
                    sink.complete();
                } catch (Throwable e) {
                    sink.error(ResultStatusConsumerException.create(statusConsumer, e));
                }
            }

        }


        private static IllegalStateException createDuplicateSubscribe(int index) {
            String message = String.format("Query result[index:%s] can only subscribe one time.", index);
            return new IllegalStateException(message);
        }


    } // QuerySinkImpl


    private static final class SkipQuerySink implements QuerySink {

        private static final SkipQuerySink INSTANCE = new SkipQuerySink();

        private SkipQuerySink() {
        }

        @Override
        public final void accept(ResultStatus resultStatus) {
            //no-op
        }

        @Override
        public final void complete() {
            //no-op
        }

        @Override
        public final void next(ResultRow resultRow) {
            //no-op
        }

        @Override
        public final boolean isCancelled() {
            return true;
        }

    }


}
