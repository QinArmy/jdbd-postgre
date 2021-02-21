package io.jdbd.vendor.result;

import io.jdbd.*;
import io.jdbd.vendor.DirtyFluxSink;
import io.jdbd.vendor.JdbdCompositeException;
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

import java.util.*;
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

    private static final Consumer<ResultStates> EMPTY_CONSUMER = t -> {
    };


    private final TaskAdjutant adjutant;

    private final Consumer<Object> subscribeConsumer;

    private final MultiResults multiResults;

    private boolean publishSubscribeEvent;

    private Queue<RealDownstreamSink> realSinkQueue;

    private Queue<BufferDownstreamSink> bufferSinkQueue;

    private ResultStates lastResultStates;

    private DownstreamSink currentSink;

    private List<Throwable> errorList;

    private int resultSequenceId = 1;

    private JdbdMultiResultsSink(TaskAdjutant adjutant, Consumer<Object> subscribeConsumer) {
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
        this.resultSequenceId++;

        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            // firstly
            currentSink.nextUpdate(resultStates);

        } else if (JdbdCollectionUtils.isEmpty(this.errorList)) {
            final DownstreamSink realSink = pollRealSink();
            if (realSink == null) {
                addBufferDownstreamSink(new BufferUpdateSink(resultStates));
            } else {
                this.currentSink = realSink;
                realSink.nextUpdate(resultStates);
            }
        }

        if (this.currentSink == null) {
            this.currentSink = pollRealSink();
        }

    }

    @Override
    public QuerySink nextQuery() throws IllegalStateException {
        assertMultiResultNotEnd();

        this.resultSequenceId++;

        final QuerySink querySink;
        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            querySink = currentSink.nextQuery();
        } else if (!JdbdCollectionUtils.isEmpty(this.errorList)) {
            //has error ignore result
            LOG.debug("occur error,ignore query results.");
            DownstreamSink tempSink = new RealQuerySink(DirtyFluxSink.INSTANCE, status -> { /* no-op */ });
            this.currentSink = tempSink;
            querySink = tempSink.nextQuery();
        } else {
            DownstreamSink tempSink = pollRealSink();
            if (tempSink == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("no {},create {} .", RealQuerySink.class.getName(), BufferQuerySink.class.getName());
                }
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

    @Nullable
    private RealDownstreamSink pollRealSink() {
        drainReceiver();
        Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        return realSinkQueue == null ? null : realSinkQueue.poll();
    }

    private void internalError(Throwable e) {
        if (JdbdCollectionUtils.isEmpty(this.errorList)) {
            addError(e);
            DownstreamSink currentSink = this.currentSink;
            if (currentSink != null) {
                this.currentSink = null;
                currentSink.error(e);
            }
        } else {
            addError(e);
        }
        emitErrorToRealSinkQueue();
    }


    private void assertMultiResultNotEnd() {
        ResultStates resultStates = this.lastResultStates;
        if (resultStates != null && !resultStates.hasMoreResults()) {
            throw new IllegalStateException("MultiResult have ended.");
        }
    }


    private void emitErrorToRealSinkQueue() {
        final Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        if (realSinkQueue != null && !JdbdCollectionUtils.isEmpty(this.errorList)) {
            Throwable e = createException();
            RealDownstreamSink downstreamSink;
            while ((downstreamSink = realSinkQueue.poll()) != null) {
                downstreamSink.error(e);
            }
            this.realSinkQueue = null;
            clearBufferQueue();
        }
    }

    private void clearBufferQueue() {
        final Queue<BufferDownstreamSink> bufferSinkQueue = this.bufferSinkQueue;
        if (bufferSinkQueue != null) {
            BufferDownstreamSink bufferSink;
            while ((bufferSink = bufferSinkQueue.poll()) != null) {
                bufferSink.clearBuffer();
            }
            this.bufferSinkQueue = null;
        }
    }


    /**
     * @see DefaultMultiResults#nextUpdate()
     */
    private void subscribeNextUpdate(final MonoSink<ResultStates> sink) {
        ResultStates resultStates = this.lastResultStates;

        if (!JdbdCollectionUtils.isEmpty(this.errorList)) {
            sink.error(createException());
        } else if (resultStates != null && !resultStates.hasMoreResults()) {
            sink.error(new NoMoreResultException("MultiResults have ended."));
        } else if (!JdbdCollectionUtils.isEmpty(this.realSinkQueue)
                || !JdbdCollectionUtils.isEmpty(this.bufferSinkQueue)) {
            addRealDownstreamSink(new RealUpdateSink(sink));
            drainReceiver();
        } else if (this.currentSink == null) {
            LOG.debug("this.currentSink is null, set this.currentSink = RealUpdateSink");
            this.currentSink = new RealUpdateSink(sink);
            if (!this.publishSubscribeEvent) {
                this.subscribeConsumer.accept(null);
                this.publishSubscribeEvent = true;
            }

        } else {
            LOG.debug("this.currentSink isn't null,add RealUpdateSink");
            addRealDownstreamSink(new RealUpdateSink(sink));
        }
    }

    /**
     * @see DefaultMultiResults#nextQuery(Consumer)
     */
    private void subscriberNextQuery(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {
        final ResultStates resultStates = this.lastResultStates;
        final DownstreamSink currentSink = this.currentSink;

        if (!JdbdCollectionUtils.isEmpty(this.errorList)) {
            sink.error(createException());
        } else if (resultStates != null && !resultStates.hasMoreResults()) {
            sink.error(new NoMoreResultException("MultiResults have ended."));
        } else if (!JdbdCollectionUtils.isEmpty(this.realSinkQueue)
                || !JdbdCollectionUtils.isEmpty(this.bufferSinkQueue)) {
            addRealDownstreamSink(new RealQuerySink(sink, statesConsumer));
            drainReceiver();
        } else if (currentSink == null) {
            LOG.debug("this.currentSink is null, set this.currentSink = RealUpdateSink");
            this.currentSink = new RealQuerySink(sink, statesConsumer);
            if (!this.publishSubscribeEvent) {
                this.subscribeConsumer.accept(null);
                this.publishSubscribeEvent = true;
            }
        } else if (currentSink instanceof BufferQuerySink) {
            BufferQuerySink bufferQuerySink = (BufferQuerySink) currentSink;
            if (bufferQuerySink.statesConsumer == null) {
                LOG.debug("this.currentSink isn't null,but BufferQuerySink actualSink not set,replace buffer.");
                bufferQuerySink.subscribe(sink, statesConsumer);
            } else {
                LOG.debug("this.currentSink isn't null,add RealQuerySink");
                addRealDownstreamSink(new RealQuerySink(sink, statesConsumer));
            }

        } else {
            LOG.debug("this.currentSink isn't null,add RealQuerySink");
            addRealDownstreamSink(new RealQuerySink(sink, statesConsumer));
        }

    }

    private void drainReceiver() {
        final Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        final Queue<BufferDownstreamSink> bufferSinkQueue = this.bufferSinkQueue;
        if (realSinkQueue == null || bufferSinkQueue == null) {
            return;
        }
        final int size = Math.min(realSinkQueue.size(), bufferSinkQueue.size());
        RealDownstreamSink realSink;
        BufferDownstreamSink bufferSink;

        for (int i = 0; i < size; i++) {
            realSink = realSinkQueue.poll();
            bufferSink = bufferSinkQueue.poll();
            if (bufferSink instanceof BufferUpdateSink && realSink instanceof RealUpdateSink) {
                realSink.nextUpdate(((BufferUpdateSink) bufferSink).resultStates);
            } else if (bufferSink instanceof BufferQuerySink && realSink instanceof RealQuerySink) {
                RealQuerySink realQuerySink = (RealQuerySink) realSink;
                ((BufferQuerySink) bufferSink).drainToDownstream(realQuerySink.sinkWrapper.actualSink
                        , realQuerySink.statesConsumer);
            } else if (bufferSink instanceof BufferUpdateSink) {
                bufferSink.clearBuffer();
                if (realSink != null) {
                    Throwable e = ErrorSubscribeException.errorSubscribe(UPDATE, QUERY
                            , "Expect subscribe nextUpdate() but subscribe nextQuery()");
                    addError(e);
                    realSink.error(e);
                }
            } else if (bufferSink instanceof BufferQuerySink) {
                bufferSink.clearBuffer();
                if (realSink != null) {
                    Throwable e = ErrorSubscribeException.errorSubscribe(QUERY, UPDATE
                            , "Expect subscribe nextQuery() but subscribe nextUpdate()");
                    addError(e);
                    realSink.error(e);
                }
            } else {
                throw new IllegalStateException(String.format("Unknown %s type.", bufferSink));
            }
        }

        if (!JdbdCollectionUtils.isEmpty(this.errorList)) {
            emitErrorToRealSinkQueue();
        }
    }

    private Throwable createException() {
        List<Throwable> errorList = this.errorList;
        if (JdbdCollectionUtils.isEmpty(errorList)) {
            throw new IllegalStateException("No error.");
        }
        Throwable e;
        if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList
                    , "MultiResults read occur multi error,the first error[%s]", errorList.get(0).getMessage());
        }
        return e;

    }

    /*################################## blow private instance inner class ##################################*/

    private interface DownstreamSink {

        void error(Throwable e);

        void nextUpdate(ResultStates resultStates);

        QuerySink nextQuery() throws IllegalStateException;

    }

    private interface BufferDownstreamSink extends DownstreamSink {

        void clearBuffer() throws IllegalStateException;
    }


    private interface RealDownstreamSink extends DownstreamSink {


    }

    private interface DownstreamQuerySink extends DownstreamSink {

        void acceptStatus(ResultStates resultStates);
    }


    private final class BufferQuerySink implements BufferDownstreamSink, DownstreamQuerySink {

        private final BufferFluxSink bufferFluxSink;

        private final QuerySink querySink;

        private Consumer<ResultStates> statesConsumer;

        private ResultStates resultStates;

        private boolean started;

        private BufferQuerySink() {
            this.bufferFluxSink = new BufferFluxSink(this);
            this.querySink = new DefaultQuerySink(this, this.bufferFluxSink);
        }

        @Override
        public void clearBuffer() {
            this.bufferFluxSink.clearQueue();
            this.resultStates = null;
        }

        @Override
        public void error(Throwable e) {
            this.bufferFluxSink.internalError(e);
        }

        /**
         * @see JdbdMultiResultsSink#drainReceiver()
         */
        private void drainToDownstream(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            this.statesConsumer = statesConsumer;
            this.bufferFluxSink.drainToDownstream(sink);
            ResultStates resultStates = Objects.requireNonNull(this.resultStates, "this.resultStates");
            try {
                statesConsumer.accept(resultStates);
                this.bufferFluxSink.internalComplete();
            } catch (Throwable e) {
                this.bufferFluxSink.internalError(new ResultStateConsumerException(e, "ResultStatus consumer error."));
            }

        }

        /**
         * @see JdbdMultiResultsSink#subscriberNextQuery(FluxSink, Consumer)
         */
        private void subscribe(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            this.statesConsumer = statesConsumer;
            this.bufferFluxSink.drainToDownstream(sink);
        }

        @Override
        public void nextUpdate(ResultStates resultStates) {
            if (JdbdMultiResultsSink.this.currentSink == this) {
                throw new IllegalStateException(String.format("Current DownstreamSink[%s] isn't update sink.", this));
            } else {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }

        }

        @Override
        public QuerySink nextQuery() throws IllegalStateException {
            if (JdbdMultiResultsSink.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.started) {
                throw new IllegalStateException(String.format("%s have started.", this));
            }
            this.started = true;
            return this.querySink;
        }


        @Override
        public void acceptStatus(final ResultStates resultStates) throws IllegalStateException {
            Consumer<ResultStates> statesConsumer = this.statesConsumer;
            if (statesConsumer == null) {
                this.resultStates = resultStates;
            } else {
                statesConsumer.accept(resultStates);
            }
        }
    }

    private class RealQuerySink implements RealDownstreamSink, DownstreamQuerySink {

        private final FluxSinkWrapper sinkWrapper;

        private final Consumer<ResultStates> statesConsumer;

        private final QuerySink querySink;

        private boolean started;

        private RealQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            this.sinkWrapper = new FluxSinkWrapper(this, sink);
            this.statesConsumer = statesConsumer;
            this.querySink = new DefaultQuerySink(this, this.sinkWrapper);
        }

        @Override
        public void error(Throwable e) {
            this.sinkWrapper.internalError(e);
        }

        @Override
        public void nextUpdate(ResultStates resultStates) {
            if (JdbdMultiResultsSink.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            LOG.debug("{} expect Query but update.", this);
            // here, downstream subscribe error,should subscribe io.jdbd.MultiResults.nextUpdate.
            ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(QUERY, UPDATE
                    , "Result sequenceId[%s] Expect subscribe nextQuery,but subscribe nextUpdate."
                    , JdbdMultiResultsSink.this.resultSequenceId);
            JdbdMultiResultsSink.this.internalError(e);
        }

        @Override
        public QuerySink nextQuery() throws IllegalStateException {
            if (JdbdMultiResultsSink.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.started) {
                throw new IllegalStateException(String.format("%s have started.", this));
            }
            this.started = true;
            return this.querySink;
        }

        @Override
        public void acceptStatus(ResultStates resultStates) {
            this.statesConsumer.accept(resultStates);
        }
    }

    private final class RealUpdateSink implements RealDownstreamSink, DownstreamQuerySink {

        private final MonoSink<ResultStates> sink;

        private boolean errorSubscribe;

        private RealUpdateSink(MonoSink<ResultStates> sink) {
            this.sink = sink;
        }

        @Override
        public void error(Throwable e) {
            this.sink.error(e);
        }

        @Override
        public void nextUpdate(ResultStates resultStates) {
            if (JdbdMultiResultsSink.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            JdbdMultiResultsSink.this.currentSink = null;
            this.sink.success(resultStates);
        }

        @Override
        public QuerySink nextQuery() {
            if (JdbdMultiResultsSink.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.errorSubscribe) {
                throw new IllegalStateException(String.format("%s have started.", this));
            }
            this.errorSubscribe = true;
            // here, downstream subscribe error,should subscribe io.jdbd.MultiResults.nextUpdate.
            ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(UPDATE, QUERY
                    , "Result sequenceId[%s] Expect subscribe nextQuery,but subscribe nextUpdate."
                    , JdbdMultiResultsSink.this.resultSequenceId++);
            JdbdMultiResultsSink.this.internalError(e);

            return new DefaultQuerySink(this, new FluxSinkWrapper(this, DirtyFluxSink.INSTANCE));
        }

        @Override
        public void acceptStatus(ResultStates resultStates) {
            // no-op
        }
    }


    private final class DefaultQuerySink implements QuerySink {

        private final DownstreamQuerySink downstreamSink;

        private final FluxSink<ResultRow> sinkWrapper;

        private ResultStates resultStates;

        private DefaultQuerySink(DownstreamQuerySink downstreamSink, FluxSink<ResultRow> sinkWrapper) {
            this.downstreamSink = downstreamSink;
            this.sinkWrapper = sinkWrapper;
        }

        @Override
        public FluxSink<ResultRow> getSink() {
            return this.sinkWrapper;
        }

        @Override
        public void acceptStatus(final ResultStates resultStates) throws IllegalStateException {
            if (this.resultStates != null) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            this.resultStates = resultStates;
            JdbdMultiResultsSink.this.lastResultStates = resultStates;
            this.downstreamSink.acceptStatus(resultStates);
        }

    }

    private final class FluxSinkWrapper implements FluxSink<ResultRow> {

        private final DownstreamSink downstreamSink;

        private final FluxSink<ResultRow> actualSink;

        private FluxSinkWrapper(DownstreamSink downstreamSink, FluxSink<ResultRow> actualSink) {
            this.downstreamSink = downstreamSink;
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
            if (currentSink != this.downstreamSink) {
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
            if (currentSink != this.downstreamSink) {
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

        private Queue<ResultRow> resultRowQueue;

        private FluxSink<ResultRow> actualSink;

        private BufferFluxSink(BufferQuerySink bufferQuerySink) {
            this.bufferQuerySink = bufferQuerySink;
        }


        private void clearQueue() {
            if (JdbdCollectionUtils.isEmpty(JdbdMultiResultsSink.this.errorList)) {
                throw new IllegalStateException("No error ,reject clear queue.");
            }
            Queue<ResultRow> resultRowQueue = this.resultRowQueue;
            if (resultRowQueue != null) {
                resultRowQueue.clear();
                this.resultRowQueue = null;
            }
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink != null) {
                actualSink.error(JdbdMultiResultsSink.this.createException());
            }
        }

        private void internalError(Throwable e) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink != null) {
                this.actualSink.error(e);
            }
        }

        private void drainToDownstream(FluxSink<ResultRow> sink) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink != null) {
                throw new IllegalStateException(
                        String.format("%s actualSink isn't null,can't invoke drainToDownstream() method", this))
            }
            this.actualSink = sink;
            actualSink = sink;
            Queue<ResultRow> resultRowQueue = this.resultRowQueue;
            if (resultRowQueue != null) {
                ResultRow resultRow;
                while ((resultRow = resultRowQueue.poll()) != null) {
                    actualSink.next(resultRow);
                }
            }

        }

        private void internalComplete() {
            Objects.requireNonNull(this.actualSink, "this.actualSink")
                    .complete();

        }

        @Override
        public FluxSink<ResultRow> next(ResultRow resultRow) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink == null) {
                Queue<ResultRow> resultRowQueue = this.resultRowQueue;
                if (resultRowQueue == null) {
                    resultRowQueue = new ArrayDeque<>();
                    this.resultRowQueue = resultRowQueue;
                }
                resultRowQueue.add(resultRow);
            } else {
                actualSink.next(resultRow);
            }
            return this;
        }


        @Override
        public void complete() {
            if (JdbdMultiResultsSink.this.currentSink != this.bufferQuerySink) {
                throw new IllegalStateException("Current FluxSink have ended.");
            }
            // firstly
            JdbdMultiResultsSink.this.currentSink = null;
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink != null) {
                // secondly
                this.actualSink.complete();
            }
        }

        @Override
        public void error(Throwable e) {
            if (JdbdMultiResultsSink.this.currentSink != this.bufferQuerySink) {
                throw new IllegalStateException("Current FluxSink have ended.");
            }
            JdbdMultiResultsSink.this.internalError(e);
        }

        @Override
        public Context currentContext() {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink == null ? Context.empty() : actualSink.currentContext();
        }

        @Override
        public long requestedFromDownstream() {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink == null ? Long.MAX_VALUE : actualSink.requestedFromDownstream();
        }

        @Override
        public boolean isCancelled() {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink != null && actualSink.isCancelled();
        }

        @Override
        public FluxSink<ResultRow> onRequest(LongConsumer consumer) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink == null ? this : actualSink.onRequest(consumer);
        }

        @Override
        public FluxSink<ResultRow> onCancel(Disposable d) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink == null ? this : actualSink.onCancel(d);
        }

        @Override
        public FluxSink<ResultRow> onDispose(Disposable d) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink == null ? this : actualSink.onDispose(d);
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


        @Override
        public void clearBuffer() {
            //no-op
        }
    }


}
