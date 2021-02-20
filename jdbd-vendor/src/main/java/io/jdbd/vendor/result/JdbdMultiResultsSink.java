package io.jdbd.vendor.result;

import io.jdbd.ErrorSubscribeException;
import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.vendor.DirtyFluxSink;
import io.jdbd.vendor.MultiResultsSink;
import io.jdbd.vendor.TaskAdjutant;
import io.jdbd.vendor.util.JdbdCollectionUtils;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.jdbd.ErrorSubscribeException.SubscriptionType.QUERY;
import static io.jdbd.ErrorSubscribeException.SubscriptionType.UPDATE;

public final class JdbdMultiResultsSink implements MultiResultsSink {


    public static Pair<MultiResultsSink, MultiResults> create(TaskAdjutant adjutant
            , Consumer<Boolean> subscribeConsumer) {
        JdbdMultiResultsSink sink = new JdbdMultiResultsSink(adjutant, subscribeConsumer);
        return new Pair<>(sink, sink.multiResults);
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbdMultiResultsSink.class);


    private final TaskAdjutant adjutant;

    private final Consumer<Boolean> subscribeConsumer;

    private final MultiResults multiResults;

    private Queue<RealDownstreamSink> realSinkQueue;

    private Queue<BufferDownstreamSink> bufferSinkQueue;

    private ResultStates lastResultStates;

    private DownstreamSink currentSink;

    private List<Throwable> errorList;

    private int resultSequenceId = 1;

    private JdbdMultiResultsSink(TaskAdjutant adjutant, Consumer<Boolean> subscribeConsumer) {
        this.adjutant = adjutant;
        this.subscribeConsumer = subscribeConsumer;
        this.multiResults = new DefaultMultiResults(this);
    }


    @Override
    public void error(final Throwable e) {
        this.internalError(e);
    }


    @Override
    public void nextUpdate(final ResultStates resultStates) throws IllegalStateException {
        assertMultiResultNotEnd();
        this.lastResultStates = resultStates;

        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            // firstly
            currentSink.nextUpdate(resultStates);
            // secondly
            // @see io.jdbd.vendor.result.JdbdMultiResultsSink.RealQuerySink.nextUpdate
            // @see io.jdbd.vendor.result.JdbdMultiResultsSink.BufferQuerySink.nextUpdate
            // @see io.jdbd.vendor.result.JdbdMultiResultsSink.internalError
            this.currentSink = pollRealSink();
        } else if (JdbdCollectionUtils.isEmpty(this.errorList)) {
            DownstreamSink realSink = pollRealSink();
            if (realSink == null) {
                addBufferDownstreamSink(new BufferUpdateSink(resultStates));
            } else {
                realSink.nextUpdate(resultStates);
            }
        }

    }

    @Override
    public QuerySink nextQuery() throws IllegalStateException {
        assertMultiResultNotEnd();

        final QuerySink querySink;
        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            querySink = currentSink.nextQuery();
        } else if (!JdbdCollectionUtils.isEmpty(this.errorList)) {
            //has error ignore result
            DownstreamSink tempSink = new RealQuerySink(DirtyFluxSink.INSTANCE, status -> { /* no-op */ });
            this.currentSink = tempSink;
            querySink = tempSink.nextQuery();
        } else {
            DownstreamSink tempSink = pollRealSink();
            if (tempSink == null) {
                tempSink = new BufferQuerySink();
            }
            this.currentSink = tempSink;
            querySink = tempSink.nextQuery();
        }
        return querySink;
    }



    /*################################## blow private method ##################################*/

    private void addError(Throwable e) {
        List<Throwable> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
        }
        errorList.add(e);
    }

    private void addBufferDownstreamSink(BufferDownstreamSink bufferSink) {
        Queue<BufferDownstreamSink> bufferSinkQueue = this.bufferSinkQueue;
        if (bufferSinkQueue == null) {
            bufferSinkQueue = new ArrayDeque<>();
        }
        bufferSinkQueue.add(bufferSink);
    }

    private void addRealDownstreamSink(RealDownstreamSink realSink) {
        Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        if (realSinkQueue == null) {
            realSinkQueue = new ArrayDeque<>();
        }
        realSinkQueue.add(realSink);
    }

    private void internalError(Throwable e) {
        if (JdbdCollectionUtils.isEmpty(this.errorList)) {
            addError(e);
            DownstreamSink currentSink = this.currentSink;
            if (currentSink != null) {
                this.currentSink = null;
                currentSink.error(e);
            }

            emitErrorToRealSinkQueue(e);
        } else {
            addError(e);
        }

    }

    @Nullable
    private RealDownstreamSink pollRealSink() {
        Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        return realSinkQueue == null ? null : realSinkQueue.poll();
    }

    private void assertMultiResultNotEnd() {
        ResultStates resultStates = this.lastResultStates;
        if (resultStates != null && !resultStates.hasMoreResults()) {
            throw new IllegalStateException("MultiResult have ended.");
        }
    }


    private void emitErrorToRealSinkQueue(Throwable e) {
        final Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        if (realSinkQueue != null) {
            RealDownstreamSink downstreamSink;
            while ((downstreamSink = realSinkQueue.poll()) != null) {
                downstreamSink.error(e);
            }
            this.realSinkQueue = null;
        }
    }


    private void subscribeNextUpdate(MonoSink<ResultStates> sink) {

    }

    private void subscriberNextQuery(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {

    }

    /*################################## blow private instance inner class ##################################*/

    private interface DownstreamSink {

        void error(Throwable e);

        void nextUpdate(ResultStates resultStates);

        QuerySink nextQuery() throws IllegalStateException;


    }

    private interface BufferDownstreamSink extends DownstreamSink {

    }


    private interface RealDownstreamSink extends DownstreamSink {


    }


    private abstract class AbstractQuerySink implements DownstreamSink, QuerySink {

        private boolean started;

        private ResultStates resultStates;

        private final Consumer<ResultStates> statesConsumer;

        private AbstractQuerySink(Consumer<ResultStates> statesConsumer) {
            this.statesConsumer = statesConsumer;
        }

        @Override
        public final void nextUpdate(ResultStates resultStates) {
            // here, downstream subscribe error,should subscribe io.jdbd.MultiResults.nextUpdate.
            ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(QUERY, UPDATE
                    , "Result sequenceId[%s] Expect subscribe nextQuery,but subscribe nextUpdate."
                    , JdbdMultiResultsSink.this.resultSequenceId++);
            JdbdMultiResultsSink.this.internalError(e);
        }

        @Override
        public final QuerySink nextQuery() throws IllegalStateException {
            if (this.started) {
                throw new IllegalStateException(String.format("%s have started.", this))
            }
            this.started = true;
            return this;
        }

        @Override
        public final void acceptStatus(ResultStates resultStates) throws IllegalStateException {
            if (this.resultStates != null) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            this.resultStates = resultStates;
            JdbdMultiResultsSink.this.lastResultStates = resultStates;
            this.statesConsumer.accept(resultStates);
        }

    }


    private final class BufferQuerySink extends AbstractQuerySink implements BufferDownstreamSink, QuerySink {

        private final BufferFluxSink bufferFluxSink;

        private BufferQuerySink() {
            super(resultStates -> {
            });
            this.bufferFluxSink = new BufferFluxSink(this);
        }

        @Override
        public void error(Throwable e) {
            //no-op
        }

        @Override
        public FluxSink<ResultRow> getSink() {
            return this.bufferFluxSink;
        }

    }


    private class RealQuerySink extends AbstractQuerySink implements RealDownstreamSink, QuerySink {

        private final FluxSinkWrapper sinkWrapper;


        private RealQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            super(statesConsumer);
            this.sinkWrapper = new FluxSinkWrapper(this, sink);
        }

        @Override
        public void error(Throwable e) {
            this.sinkWrapper.internalError(e);
        }

        @Override
        public FluxSink<ResultRow> getSink() {
            return this.sinkWrapper;
        }
    }

    private final class FluxSinkWrapper implements FluxSink<ResultRow> {

        private final RealQuerySink querySink;

        private final FluxSink<ResultRow> actualSink;

        private FluxSinkWrapper(RealQuerySink querySink, FluxSink<ResultRow> actualSink) {
            this.querySink = querySink;
            this.actualSink = actualSink;
        }

        @Override
        public FluxSink<ResultRow> next(ResultRow resultRow) {
            this.actualSink.next(resultRow);
            return this;
        }

        @Override
        public void complete() {
            final DownstreamSink currentSink = JdbdMultiResultsSink.this.currentSink;
            if (currentSink != this.querySink) {
                throw new IllegalStateException("Current FluxSink have ended.");
            }
            // firstly
            JdbdMultiResultsSink.this.currentSink = null;
            // secondly
            this.actualSink.complete();
        }

        private void internalError(Throwable e) {
            this.actualSink.error(e);
        }

        @Override
        public void error(final Throwable e) {
            final DownstreamSink currentSink = JdbdMultiResultsSink.this.currentSink;
            if (currentSink != this.querySink) {
                throw new IllegalStateException("Current FluxSink have ended.");
            }
            JdbdMultiResultsSink.this.internalError(e);
        }

        @Override
        public Context currentContext() {
            return this.actualSink.currentContext();
        }

        @Override
        public long requestedFromDownstream() {
            return this.actualSink.requestedFromDownstream();
        }

        @Override
        public boolean isCancelled() {
            return !JdbdCollectionUtils.isEmpty(JdbdMultiResultsSink.this.errorList)
                    || this.actualSink.isCancelled();
        }

        @Override
        public FluxSink<ResultRow> onRequest(LongConsumer consumer) {
            this.actualSink.onRequest(consumer);
            return this;
        }

        @Override
        public FluxSink<ResultRow> onCancel(Disposable d) {
            this.actualSink.onCancel(d);
            return this;
        }

        @Override
        public FluxSink<ResultRow> onDispose(Disposable d) {
            this.actualSink.onDispose(d);
            return this;
        }
    }

    private final class BufferFluxSink implements FluxSink<ResultRow> {

        private final BufferQuerySink bufferQuerySink;

        private BufferFluxSink(BufferQuerySink bufferQuerySink) {
            this.bufferQuerySink = bufferQuerySink;
        }

        @Override
        public FluxSink<ResultRow> next(ResultRow resultRow) {
            return null;
        }

        @Override
        public void complete() {

        }

        @Override
        public void error(Throwable e) {

        }

        @Override
        public Context currentContext() {
            return null;
        }

        @Override
        public long requestedFromDownstream() {
            return 0;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public FluxSink<ResultRow> onRequest(LongConsumer consumer) {
            return null;
        }

        @Override
        public FluxSink<ResultRow> onCancel(Disposable d) {
            return null;
        }

        @Override
        public FluxSink<ResultRow> onDispose(Disposable d) {
            return null;
        }
    }

    /*################################## blow private static inner class  ##################################*/


    private static class DefaultMultiResults implements MultiResults {

        private final JdbdMultiResultsSink resultsSink;

        private DefaultMultiResults(JdbdMultiResultsSink resultsSink) {
            this.resultsSink = resultsSink;
        }

        @Override
        public Publisher<ResultStates> nextUpdate() {
            return Mono.create(sink -> {
                if (resultsSink.adjutant.inEventLoop()) {
                    resultsSink.subscribeNextUpdate(sink);
                } else {
                    this.resultsSink.adjutant.execute(() -> resultsSink.subscribeNextUpdate(sink));
                }
            });
        }

        @Override
        public Publisher<ResultRow> nextQuery(final Consumer<ResultStates> statesConsumer) {
            return Flux.create(sink -> {
                if (resultsSink.adjutant.inEventLoop()) {
                    resultsSink.subscriberNextQuery(sink, statesConsumer);
                } else {
                    this.resultsSink.adjutant.execute(() -> resultsSink.subscriberNextQuery(sink, statesConsumer));
                }
            });
        }

    }

    private static final class RealUpdateSink implements RealDownstreamSink {

        private final MonoSink<ResultStates> sink;

        private RealUpdateSink(MonoSink<ResultStates> sink) {
            this.sink = sink;
        }

        @Override
        public void error(Throwable e) {
            this.sink.error(e);
        }

        @Override
        public void nextUpdate(ResultStates resultStates) {
            this.sink.success(resultStates);
        }

        @Override
        public QuerySink nextQuery() {
            throw new IllegalStateException("Current result is query,can't emit query result.");
        }
    }

    private static final class BufferUpdateSink implements BufferDownstreamSink {


        private final ResultStates resultStates;

        private BufferUpdateSink(ResultStates resultStates) {
            this.resultStates = resultStates;
        }

        @Override
        public void error(Throwable e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextUpdate(ResultStates resultStates) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QuerySink nextQuery() {
            throw new UnsupportedOperationException();
        }
    }


}
