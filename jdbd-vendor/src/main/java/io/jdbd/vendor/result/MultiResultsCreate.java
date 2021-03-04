package io.jdbd.vendor.result;

import io.jdbd.*;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.MultiResultsSink;
import io.jdbd.vendor.TaskAdjutant;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdExceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.function.Consumer;

import static io.jdbd.ResultType.QUERY;
import static io.jdbd.ResultType.UPDATE;


/**
 * <p>
 *     <ul>
 *         <li>below handle update result subscribe event
 *         <ol>
 *             <li>{@link DefaultMultiResults#nextUpdate()} </li>
 *             <li>{@link #subscribeNextUpdate(MonoSink)}</li>
 *         </ol>
 *         </li>
 *         <li>below handle query result subscribe event
 *         <ol>
 *             <li>{@link DefaultMultiResults#nextQuery(Consumer)}</li>
 *             <li>{@link #subscribeNextQuery(FluxSink, Consumer)}</li>
 *         </ol>
 *         </li>
 *     </ul>
 * </p>
 */
final class MultiResultsCreate implements MultiResultsSink {


    static ReactorMultiResults create(TaskAdjutant adjutant, Consumer<MultiResultsSink> callback) {
        return new MultiResultsCreate(adjutant, callback)
                .multiResults;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiResultsCreate.class);


    private final TaskAdjutant adjutant;

    private final Consumer<MultiResultsSink> subscribeConsumer;

    private final ReactorMultiResults multiResults;

    private volatile boolean publishSubscribeEvent;

    private Queue<RealDownstreamSink> realSinkQueue;

    private Queue<BufferDownstreamSink> bufferSinkQueue;

    /**
     * <p>
     * below can modify this field:
     * <ul>
     *     <li>{@link #nextUpdate(ResultStates)}</li>
     *     <li>{@link DefaultQuerySink#accept(ResultStates)}</li>
     * </ul>
     * </p>
     */
    private ResultStates lastResultStates;

    private DownstreamSink currentSink;

    /**
     * <p>
     * below methods can modify this field:
     * <ul>
     *     <li>{@link #addDownstreamError(JdbdException)}</li>
     * </ul>
     * </p>
     */
    private List<JdbdException> errorList;

    private JdbdException upstreamError;

    /**
     * <p>
     * below methods can modify this field:
     * <ul>
     *     <li>{@link #nextUpdate(ResultStates)}</li>
     *     <li>{@link #nextUpdate(ResultStates)}</li>
     * </ul>
     * </p>
     */
    private int resultSequenceId = 1;

    private MultiResultsCreate(TaskAdjutant adjutant, Consumer<MultiResultsSink> subscribeConsumer) {
        this.adjutant = adjutant;
        this.subscribeConsumer = subscribeConsumer;
        this.multiResults = new DefaultMultiResults(this);
    }


    @Override
    public void error(final Throwable e) throws IllegalStateException {
        if (isTerminated()) {
            // upstream bug
            throw new IllegalStateException("MultiResults is terminated,reject error,upstream bug.");
        }
        final JdbdException je = JdbdExceptions.wrap(e);
        this.upstreamError = je;
        DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            this.currentSink = null;
            currentSink.error(je);
        }
        emitErrorToSinkQueue(je);

    }


    @Override
    public void nextUpdate(final ResultStates resultStates) throws IllegalStateException {
        if (isTerminated()) {
            // upstream bug
            throw new IllegalStateException("MultiResults is terminated,reject update result,upstream bug.");
        }

        final int resultSequenceId = this.resultSequenceId;

        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            // firstly
            currentSink.nextUpdate(resultSequenceId, resultStates); //maybe throw error.
            //no throw error,update last result
            updateLastResultStates(resultStates);
        } else {
            updateLastResultStates(resultStates);
            if (hasDownstreamError()) {
                LOG.debug("MultiResults has downstream error,ignore update result[sequenceId:{}].", resultSequenceId);
            } else {
                final RealDownstreamSink realSink = pollRealSink();
                if (realSink == null) {
                    addBufferDownstreamSink(createBufferUpdateSink(resultSequenceId, resultStates));
                } else {
                    realSink.nextUpdate(resultSequenceId, resultStates);
                }
            }
        }

        // no throw error,update resultSequenceId
        this.resultSequenceId++;

        if (!hasDownstreamError() && this.currentSink == null) {
            this.currentSink = pollRealSink();
        }

    }

    @Override
    public QuerySink nextQuery() throws IllegalStateException {
        if (isTerminated()) {
            // upstream bug
            throw new IllegalStateException("MultiResults is terminated,reject query result,upstream bug.");
        }
        final int resultSequenceId = this.resultSequenceId;

        final QuerySink querySink;
        final DownstreamSink currentSink = this.currentSink;
        if (currentSink != null) {
            querySink = currentSink.nextQuery(resultSequenceId);//maybe throw error.
        } else if (hasDownstreamError()) {
            //has error ignore result
            DownstreamQuerySink tempSink = createDirtyQuerySink(resultSequenceId);
            this.currentSink = tempSink;
            querySink = tempSink.nextQuery(resultSequenceId);
        } else {
            DownstreamSink tempSink = pollRealSink();
            if (tempSink == null) {
                tempSink = createBufferQuerySink(resultSequenceId);
            }
            this.currentSink = tempSink;
            querySink = tempSink.nextQuery(resultSequenceId);
        }
        // no throw error,update resultSequenceId
        this.resultSequenceId++;
        return querySink;
    }



    /*################################## blow private method ##################################*/

    private void addDownstreamError(JdbdException e) {
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(e);
    }

    private boolean hasDownstreamError() {
        List<JdbdException> list = this.errorList;
        return list != null && !list.isEmpty();
    }

    private boolean hasUpstreamError() {
        return this.upstreamError != null;
    }

    private boolean hasError() {
        return hasUpstreamError() || hasDownstreamError();
    }

    private boolean isTerminated() {
        return this.upstreamError != null || isMultiResultEnd();
    }

    private void addBufferDownstreamSink(BufferDownstreamSink bufferSink) {
        LOG.debug("add {} to buffer sink queue.", bufferSink);
        Queue<BufferDownstreamSink> bufferSinkQueue = this.bufferSinkQueue;
        if (bufferSinkQueue == null) {
            bufferSinkQueue = new LinkedList<>();
            this.bufferSinkQueue = bufferSinkQueue;
        }
        bufferSinkQueue.add(bufferSink);
    }

    private void addRealDownstreamSink(RealDownstreamSink realSink) {
        LOG.debug("add {} to real sink queue.", realSink);

        Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        if (realSinkQueue == null) {
            realSinkQueue = new LinkedList<>();
            this.realSinkQueue = realSinkQueue;
        }
        realSinkQueue.add(realSink);
    }

    @Nullable
    private RealDownstreamSink pollRealSink() {
        //firstly drain
        drainReceiver();
        //secondly poll
        Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        return realSinkQueue == null ? null : realSinkQueue.poll();
    }


    private void updateLastResultStates(ResultStates resultStates) {
        this.lastResultStates = resultStates;
    }


    /**
     * <ol>
     *     <li>clear {@link #realSinkQueue}</li>
     *     <li>clear {@link #bufferSinkQueue}</li>
     * </ol>
     *
     * @see #drainReceiver()
     */
    private void emitErrorToSinkQueue(JdbdException e) {
        final Queue<RealDownstreamSink> realSinkQueue = this.realSinkQueue;
        if (realSinkQueue != null) {
            RealDownstreamSink downstreamSink;
            while ((downstreamSink = realSinkQueue.poll()) != null) {
                downstreamSink.error(e);
            }
            this.realSinkQueue = null;
        }

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

        if (hasError()) {
            sink.error(createException());
        } else if (isMultiResultEnd()) {
            sink.error(new NoMoreResultException("MultiResults have ended."));
        } else if (hasAnyBuffer()) {
            addRealDownstreamSink(createRealUpdateSink(sink));
            drainReceiver();
        } else if (this.currentSink == null) {
            LOG.debug("this.currentSink is null, set this.currentSink = RealUpdateSink");
            this.currentSink = createRealUpdateSink(sink);
            if (!this.publishSubscribeEvent) {
                this.publishSubscribeEvent = true;
                this.subscribeConsumer.accept(this);
            }

        } else {
            LOG.debug("this.currentSink isn't null,add RealUpdateSink");
            addRealDownstreamSink(createRealUpdateSink(sink));
        }
    }

    /**
     * @see DefaultMultiResults#nextQuery(Consumer)
     */
    private void subscribeNextQuery(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {

        final DownstreamSink currentSink = this.currentSink;

        if (hasError()) {
            sink.error(createException());
        } else if (isMultiResultEnd()) {
            sink.error(new NoMoreResultException("MultiResults have ended."));
        } else if (hasAnyBuffer()) {
            addRealDownstreamSink(createRealQuerySink(sink, statesConsumer));
            drainReceiver();
        } else if (currentSink == null) {
            LOG.debug("this.currentSink is null, set this.currentSink = RealUpdateSink");
            this.currentSink = createRealQuerySink(sink, statesConsumer);
            if (!this.publishSubscribeEvent) {
                this.publishSubscribeEvent = true;
                this.subscribeConsumer.accept(this);
            }
        } else if (currentSink instanceof BufferQuerySink) {
            BufferQuerySink bufferQuerySink = (BufferQuerySink) currentSink;
            if (bufferQuerySink.statesConsumer == null) {
                bufferQuerySink.subscribe(sink, statesConsumer);
            } else {
                addRealDownstreamSink(createRealQuerySink(sink, statesConsumer));
            }

        } else {
            LOG.debug("this.currentSink isn't null,add RealQuerySink");
            addRealDownstreamSink(createRealQuerySink(sink, statesConsumer));
        }

    }

    private boolean isMultiResultEnd() {
        final ResultStates resultStates = this.lastResultStates;
        return resultStates != null && !resultStates.hasMoreResults();
    }

    private boolean hasAnyBuffer() {
        return !JdbdCollections.isEmpty(this.realSinkQueue)
                || !JdbdCollections.isEmpty(this.bufferSinkQueue);
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
                realSink.nextUpdate(bufferSink.getSequenceId(), ((BufferUpdateSink) bufferSink).resultStates);
            } else if (bufferSink instanceof BufferQuerySink && realSink instanceof RealQuerySink) {
                RealQuerySink realQuerySink = (RealQuerySink) realSink;
                ((BufferQuerySink) bufferSink).drainToDownstream(realQuerySink.sink
                        , realQuerySink.statesConsumer);
            } else if (bufferSink instanceof BufferUpdateSink) {
                if (realSink != null) {
                    ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(UPDATE, QUERY
                            , "Update result[sequenceId(based one):%s] expect subscribe nextUpdate() but subscribe nextQuery()");
                    addDownstreamError(e);
                    realSink.error(e);
                }
                bufferSink.clearBuffer();
            } else if (bufferSink instanceof BufferQuerySink) {
                if (realSink != null) {
                    ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(QUERY, UPDATE
                            , "Query result[sequenceId(based one):%s] expect subscribe nextQuery() but subscribe nextUpdate()");
                    addDownstreamError(e);
                    realSink.error(e);
                }
                bufferSink.clearBuffer();
            } else {
                throw new IllegalStateException(String.format("Unknown %s type.", bufferSink));
            }
        }

        if (!JdbdCollections.isEmpty(this.errorList)) {
            emitErrorToSinkQueue(createException());
        }
    }

    private JdbdException createException() {
        JdbdException upstreamError = this.upstreamError;
        List<JdbdException> errorList = this.errorList;
        if (upstreamError == null && (errorList == null || errorList.isEmpty())) {
            throw new IllegalStateException("No error.");
        }
        final JdbdException e;
        if (upstreamError != null && !errorList.isEmpty()) {
            List<JdbdException> tempList = new ArrayList<>();
            tempList.add(upstreamError);
            tempList.addAll(errorList);
            e = new JdbdCompositeException(tempList
                    , "MultiResults read occur multi error,the key error[%s]", tempList.get(0).getMessage());
        } else if (upstreamError != null) {
            e = upstreamError;
        } else if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList
                    , "MultiResults read occur multi error,the first error[%s]", errorList.get(0).getMessage());
        }
        return e;

    }

    /**
     * @see #nextQuery()
     */
    private DownstreamQuerySink createDirtyQuerySink(int sequenceId) {
        LOG.debug("Occur downstream error,ignore query result[sequenceId:{}]", sequenceId);
        return new IgnoreResultQuerySink(sequenceId);
    }

    private RealQuerySink createRealQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
        return new RealQuerySink(sink, statesConsumer);
    }

    /**
     * @see #nextQuery()
     */
    private BufferQuerySink createBufferQuerySink(int sequenceId) {
        LOG.debug("Downstream not subscribe query result[sequenceId:{}],buffer result.", sequenceId);
        return new BufferQuerySink(sequenceId);
    }

    /**
     * @see #subscribeNextUpdate(MonoSink)
     */
    private RealUpdateSink createRealUpdateSink(MonoSink<ResultStates> sink) {
        return new RealUpdateSink(sink);
    }

    /**
     * @see #nextUpdate(ResultStates)
     */
    private BufferUpdateSink createBufferUpdateSink(int sequenceId, ResultStates resultStates) {
        LOG.debug("Downstream not subscribe update result[sequenceId:{}],buffer result.", sequenceId);
        return new BufferUpdateSink(resultStates, sequenceId);
    }



    /*################################## blow private static method ##################################*/

    private IllegalStateException createSequenceIdException(DownstreamSink sink) {
        return new IllegalStateException(String.format("Downstream %s sequenceId status error.", sink));
    }

    /*################################## blow private instance inner class ##################################*/

    private interface DownstreamSink {

        void error(JdbdException e);

        int getSequenceId();

        QuerySink nextQuery(int sequenceId) throws IllegalStateException;

        void nextUpdate(int sequenceId, ResultStates resultStates);

        @Override
        String toString();
    }

    private interface BufferDownstreamSink extends DownstreamSink {

        void clearBuffer() throws IllegalStateException;
    }


    private interface RealDownstreamSink extends DownstreamSink {


    }

    private interface DownstreamQuerySink extends DownstreamSink {


        /**
         * design for {@link QuerySink#accept(ResultStates)}.
         *
         * @see DefaultQuerySink#accept(ResultStates)
         */
        void accept(ResultStates resultStates);
    }


    private final class IgnoreResultQuerySink implements DownstreamSink, DownstreamQuerySink {

        private final int sequenceId;

        private QuerySink querySink;

        private IgnoreResultQuerySink(int sequenceId) {
            this.sequenceId = sequenceId;
        }

        @Override
        public int getSequenceId() {
            return this.sequenceId;
        }

        @Override
        public void error(JdbdException e) {
            //no-op
        }

        @Override
        public void nextUpdate(final int sequenceId, ResultStates resultStates) {
            throw new IllegalStateException(
                    String.format("%s not complete,reject next update result[sequenceId:%s].", this, sequenceId));
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) throws IllegalStateException {
            if (sequenceId != this.sequenceId) {
                throw new IllegalStateException(String.format("%s and sequenceId[%s] not match", this, sequenceId));
            }
            QuerySink querySink = this.querySink;
            if (querySink != null) {
                throw new IllegalStateException(
                        String.format("%s not complete,reject next query result[sequenceId:%s]."
                                , this, sequenceId));
            }
            querySink = new DefaultQuerySink(this, DirtyFluxSink.INSTANCE);
            this.querySink = querySink;
            return querySink;
        }

        @Override
        public void accept(ResultStates resultStates) {
            //no-op
        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId(based one):%s]"
                    , IgnoreResultQuerySink.class.getSimpleName(), this.sequenceId);
        }
    }


    /**
     * <p>
     * buffer query result rows,when real query sink haven't subscribed.
     * </p>
     *
     * @see #nextQuery()
     * @see RealQuerySink
     * @see #subscribeNextQuery(FluxSink, Consumer)
     */
    private final class BufferQuerySink implements BufferDownstreamSink, DownstreamQuerySink, QuerySink {

        private final int sequenceId;

        private Consumer<ResultStates> statesConsumer;

        private ResultStates resultStates;

        private FluxSink<ResultRow> actualSink;

        private Queue<ResultRow> resultRowQueue;

        private boolean started;

        private BufferQuerySink(final int sequenceId) {
            this.sequenceId = sequenceId;
        }

        @Override
        public int getSequenceId() {
            return this.sequenceId;
        }

        @Override
        public void nextUpdate(final int sequenceId, ResultStates resultStates) {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            } else {
                throw new IllegalStateException(
                        String.format("%s not complete,reject next update result[sequenceId:%s]."
                                , this, sequenceId));
            }
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) throws IllegalStateException {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (sequenceId != this.sequenceId) {
                throw new IllegalStateException(String.format("%s and sequenceId[%s] not match", this, sequenceId));
            }
            if (this.started) {
                throw new IllegalStateException(String.format("%s not complete,reject query result[%s]."
                        , this, sequenceId));
            }
            this.started = true;
            return this;
        }

        /**
         * @see #emitErrorToSinkQueue(JdbdException)
         */
        @Override
        public void clearBuffer() {
            if (!MultiResultsCreate.this.hasError()) {
                throw new IllegalStateException(String.format("No error ,%s reject clear buffer.", this));
            }
            this.resultStates = null;
            Queue<ResultRow> queue = this.resultRowQueue;
            if (queue != null) {
                queue.clear();
                this.resultRowQueue = null;
            }
        }

        @Override
        public void error(JdbdException e) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink != null) {
                actualSink.error(e);
            } else {
                clearBuffer();
            }
        }

        /**
         * @see MultiResultsCreate#drainReceiver()
         */
        private void drainToDownstream(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {
            if (this.actualSink != null || this.statesConsumer != null) {
                throw new IllegalStateException(String.format("%s have subscribed,duplication.", this));
            }
            executeDrainToDownstream(sink);

            ResultStates resultStates = Objects.requireNonNull(this.resultStates, "this.resultStates");
            try {
                statesConsumer.accept(resultStates);
                this.resultStates = null;
                sink.complete();
            } catch (Throwable e) {
                sink.error(new ResultStateConsumerException(
                        e, "ResultStatus[sequenceId(based one):%s] consumer error.", this.sequenceId));
            }

        }

        /**
         * @see MultiResultsCreate#subscribeNextQuery(FluxSink, Consumer)
         */
        private void subscribe(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.actualSink != null || this.statesConsumer != null) {
                throw new IllegalStateException(String.format("%s have subscribed,duplication.", this));
            }
            LOG.debug("this.currentSink[sequenceId(based one):{}] isn't null,but BufferQuerySink actualSink not set,replace buffer."
                    , this.sequenceId);
            this.actualSink = sink;
            this.statesConsumer = statesConsumer;

            executeDrainToDownstream(sink);

        }

        private void executeDrainToDownstream(FluxSink<ResultRow> sink) {
            Queue<ResultRow> queue = this.resultRowQueue;
            if (queue != null) {
                ResultRow resultRow;
                while ((resultRow = queue.poll()) != null) {
                    sink.next(resultRow);
                }
                this.resultRowQueue = null;
            }

        }


        @Override
        public void accept(final ResultStates resultStates) throws IllegalStateException {
            if (this.resultStates != null) {
                throw new IllegalStateException(String.format("%s Duplication ResultStates", this));
            }
            this.resultStates = resultStates;
            MultiResultsCreate.this.updateLastResultStates(resultStates);

            Consumer<ResultStates> statesConsumer = this.statesConsumer;

            if (statesConsumer != null) {
                statesConsumer.accept(resultStates);
            }
        }


        @Override
        public void complete() {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.resultStates == null) {
                throw new IllegalStateException(
                        String.format("%s Can't complete before invoke %s.accept(ResultStates resultStates)"
                                , this, QuerySink.class.getName()));
            }
            // firstly
            MultiResultsCreate.this.currentSink = null;

            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink == null) {
                MultiResultsCreate.this.addBufferDownstreamSink(this);
            } else {
                actualSink.complete();
            }
        }

        @Override
        public void next(final ResultRow resultRow) {
            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink == null) {
                Queue<ResultRow> queue = this.resultRowQueue;
                if (queue == null) {
                    queue = new LinkedList<>();
                    this.resultRowQueue = queue;
                }
                queue.add(resultRow);
            } else {
                actualSink.next(resultRow);
            }
        }

        @Override
        public boolean isCancelled() {
            FluxSink<ResultRow> actualSink = this.actualSink;
            return actualSink != null && actualSink.isCancelled();
        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId(based one):%s]", BufferQuerySink.class.getSimpleName(), this.sequenceId);
        }
    }

    private class RealQuerySink implements RealDownstreamSink, DownstreamQuerySink {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statesConsumer;

        private final DefaultQuerySink querySink;

        private int sequenceId = -1;

        private RealQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            this.sink = sink;
            this.statesConsumer = statesConsumer;
            this.querySink = new DefaultQuerySink(this, sink);
        }

        @Override
        public int getSequenceId() {
            return this.sequenceId;
        }

        @Override
        public void error(JdbdException e) {
            this.querySink.error(e);
        }

        @Override
        public void nextUpdate(final int sequenceId, ResultStates resultStates) {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.sequenceId > 0) {
                throw new IllegalStateException(String.format("%s not complete,reject next update result[sequenceId:%s]"
                        , this, this.sequenceId));
            }
            this.sequenceId = sequenceId;
            MultiResultsCreate.this.currentSink = null;
            ErrorSubscribeException e = new ErrorSubscribeException(QUERY, UPDATE
                    , "Query result[sequenceId(based one):%s] expect subscribe nextQuery ,but subscribe nextUpdate."
                    , sequenceId);

            addDownstreamError(e);

            this.sink.error(e);
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) throws IllegalStateException {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.sequenceId > 0) {
                throw new IllegalStateException(String.format("%s not complete,reject next query result[sequenceId:%s]"
                        , this, this.sequenceId));
            }
            this.sequenceId = sequenceId;
            return this.querySink;
        }

        @Override
        public void accept(ResultStates resultStates) {
            this.statesConsumer.accept(resultStates);
        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId(based one):%s]", RealQuerySink.class.getSimpleName(), this.sequenceId);
        }
    }

    private final class RealUpdateSink implements RealDownstreamSink, DownstreamQuerySink {

        private final MonoSink<ResultStates> sink;

        private int sequenceId = -1;

        private RealUpdateSink(MonoSink<ResultStates> sink) {
            this.sink = sink;
        }


        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public int getSequenceId() {
            return this.sequenceId;
        }

        @Override
        public void nextUpdate(final int sequenceId, ResultStates resultStates) {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            this.sequenceId = sequenceId;
            MultiResultsCreate.this.currentSink = null;
            this.sink.success(resultStates);
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) {
            if (MultiResultsCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.sequenceId > 0) {
                throw new IllegalStateException(String.format("%s not complete,reject next query result[sequenceId:%s]"
                        , this, this.sequenceId));
            }
            this.sequenceId = sequenceId;
            // here, downstream subscribe error,should subscribe io.jdbd.MultiResults.nextUpdate.
            ErrorSubscribeException e = ErrorSubscribeException.errorSubscribe(QUERY, UPDATE
                    , "Result[sequenceId(based one):%s] Expect subscribe nextQuery,but subscribe nextUpdate."
                    , sequenceId);

            MultiResultsCreate.this.addDownstreamError(e);
            // delay emit error . when query result complete ,emit error.
            // @see io.jdbd.vendor.result.MultiResultsCreate.DefaultQuerySink.complete
            return new DefaultQuerySink(this, DirtyFluxSink.INSTANCE);
        }

        @Override
        public void accept(ResultStates resultStates) {
            // no-op
        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId(based one):%s]", RealUpdateSink.class, this.sequenceId);
        }
    }


    private final class DefaultQuerySink implements QuerySink {

        private final DownstreamQuerySink downstreamSink;

        private final FluxSink<ResultRow> actualSink;

        private ResultStates resultStates;

        private DefaultQuerySink(DownstreamQuerySink downstreamSink, FluxSink<ResultRow> actualSink) {
            this.downstreamSink = downstreamSink;
            this.actualSink = actualSink;
        }

        @Override
        public boolean isCancelled() {
            return this.actualSink.isCancelled();
        }

        private void error(JdbdException e) {
            this.actualSink.error(e);
        }

        @Override
        public void next(ResultRow resultRow) {
            if (!MultiResultsCreate.this.hasDownstreamError()) {
                this.actualSink.next(resultRow);
            }
        }


        @Override
        public void complete() throws IllegalStateException {
            final DownstreamSink currentSink = MultiResultsCreate.this.currentSink;
            if (currentSink != this.downstreamSink) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }

            if (this.resultStates == null) {
                throw new IllegalStateException(
                        String.format("%s Can't complete before invoke %s.accept(ResultStates resultStates)"
                                , this, QuerySink.class.getName()));
            }
            // firstly
            MultiResultsCreate.this.currentSink = null;
            // secondly
            if (MultiResultsCreate.this.hasError()) {
                if (currentSink instanceof RealDownstreamSink) {
                    this.actualSink.error(MultiResultsCreate.this.createException());
                }
            } else {
                this.actualSink.complete();
            }

        }

        @Override
        public void accept(final ResultStates resultStates) throws IllegalStateException {
            if (this.resultStates != null) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            this.resultStates = resultStates;
            MultiResultsCreate.this.updateLastResultStates(resultStates);
            if (!MultiResultsCreate.this.hasError()) {
                this.downstreamSink.accept(resultStates);
            }

        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId(based one):%s]", DefaultQuerySink.class.getSimpleName()
                    , this.downstreamSink.getSequenceId());
        }
    }



    /*################################## blow private static inner class  ##################################*/


    private static final class DefaultMultiResults implements ReactorMultiResults {

        private final MultiResultsCreate resultsSink;

        private DefaultMultiResults(MultiResultsCreate resultsSink) {
            this.resultsSink = resultsSink;
        }

        @Override
        public Mono<ResultStates> nextUpdate() {
            return Mono.create(sink -> {
                if (resultsSink.adjutant.inEventLoop()) {
                    resultsSink.subscribeNextUpdate(sink);
                } else {
                    this.resultsSink.adjutant.execute(() -> resultsSink.subscribeNextUpdate(sink));
                }
            });
        }

        @Override
        public Flux<ResultRow> nextQuery(final Consumer<ResultStates> statesConsumer) {
            return Flux.create(sink -> {
                if (resultsSink.adjutant.inEventLoop()) {
                    resultsSink.subscribeNextQuery(sink, statesConsumer);
                } else {
                    this.resultsSink.adjutant.execute(() -> resultsSink.subscribeNextQuery(sink, statesConsumer));
                }
            });
        }

        @Override
        public Flux<ResultRow> nextQuery() {
            return this.nextQuery(EMPTY_CONSUMER);
        }
    }


    private static final class BufferUpdateSink implements BufferDownstreamSink {


        private final ResultStates resultStates;

        private final int sequenceId;

        private BufferUpdateSink(ResultStates resultStates, int sequenceId) {
            this.resultStates = resultStates;
            this.sequenceId = sequenceId;
        }

        @Override
        public int getSequenceId() {
            return this.sequenceId;
        }


        @Override
        public void error(JdbdException e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QuerySink nextQuery(int sequenceId) throws IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextUpdate(int sequenceId, ResultStates resultStates) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearBuffer() {
            //no-op
        }

        @Override
        public String toString() {
            return String.format("%s[sequenceId:%s]", BufferUpdateSink.class, this.sequenceId);
        }
    }


}
