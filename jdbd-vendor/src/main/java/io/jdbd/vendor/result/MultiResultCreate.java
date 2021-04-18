package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.ResultStateConsumerException;
import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.ErrorSubscribeException;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.task.TaskAdjutant;
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

import static io.jdbd.stmt.ResultType.QUERY;
import static io.jdbd.stmt.ResultType.UPDATE;


/**
 * <p>
 *     <ul>
 *         <li>below handle update result subscribe event
 *         <ol>
 *             <li>{@link DefaultMultiResult#nextUpdate()} </li>
 *             <li>{@link #subscribeNextUpdate(MonoSink)}</li>
 *         </ol>
 *         </li>
 *         <li>below handle query result subscribe event
 *         <ol>
 *             <li>{@link DefaultMultiResult#nextQuery(Consumer)}</li>
 *             <li>{@link #subscribeNextQuery(FluxSink, Consumer)}</li>
 *         </ol>
 *         </li>
 *     </ul>
 * </p>
 *
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 */
final class MultiResultCreate implements MultiResultSink {


    static ReactorMultiResult create(TaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return new MultiResultCreate(adjutant, callback)
                .multiResults;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiResultCreate.class);


    private final TaskAdjutant adjutant;

    private final Consumer<MultiResultSink> subscribeConsumer;

    private final ReactorMultiResult multiResults;

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
    private List<JdbdException> downstreamErrorList;

    private JdbdException upstreamError;

    /**
     * <p>
     * result sequence id,based zero
     * </p>
     * <p>
     * below methods can modify this field:
     * <ul>
     *     <li>{@link #nextUpdate(ResultStates)}</li>
     * </ul>
     * </p>
     */
    private int resultSequenceId = 1;

    private MultiResultCreate(TaskAdjutant adjutant, Consumer<MultiResultSink> subscribeConsumer) {
        this.adjutant = adjutant;
        this.subscribeConsumer = subscribeConsumer;
        this.multiResults = new DefaultMultiResult(this);
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
                    this.currentSink = realSink;
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
        List<JdbdException> errorList = this.downstreamErrorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.downstreamErrorList = errorList;
        }
        errorList.add(e);
    }

    private boolean hasDownstreamError() {
        List<JdbdException> list = this.downstreamErrorList;
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
     * @see DefaultMultiResult#nextUpdate()
     */
    private void subscribeNextUpdate(final MonoSink<ResultStates> sink) {

        if (hasError()) {
            sink.error(createException());
        } else if (hasAnyBuffer()) {
            LOG.debug("this.currentSink non null,add to downstream queue.");
            addRealDownstreamSink(createRealUpdateSink(sink));
            drainReceiver();
        } else if (isMultiResultEnd()) {
            String m = String.format("MultiResults have ended,resultSequenceId(based 1):%s", this.resultSequenceId);
            sink.error(new NoMoreResultException(m));
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
     * @see DefaultMultiResult#nextQuery(Consumer)
     */
    private void subscribeNextQuery(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {

        final DownstreamSink currentSink = this.currentSink;

        if (hasError()) {
            sink.error(createException());
        } else if (hasAnyBuffer()) {
            LOG.debug("this.currentSink non null,add to downstream queue.");
            addRealDownstreamSink(createRealQuerySink(sink, statesConsumer));
            drainReceiver();
        } else if (isMultiResultEnd()) {
            String m = String.format("MultiResults have ended,resultSequenceId(based 1):%s", this.resultSequenceId);
            sink.error(new NoMoreResultException(m));
        } else if (currentSink == null) {
            LOG.debug("this.currentSink is null, set this.currentSink = RealQuerySink");
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
                ((RealUpdateSink) realSink).fromBuffer((BufferUpdateSink) bufferSink);
            } else if (bufferSink instanceof BufferQuerySink && realSink instanceof RealQuerySink) {
                RealQuerySink realQuerySink = (RealQuerySink) realSink;
                ((BufferQuerySink) bufferSink).drainToDownstream(realQuerySink.sink
                        , realQuerySink.statesConsumer);
            } else if (bufferSink instanceof BufferUpdateSink) {
                if (realSink != null) {
                    ErrorSubscribeException e = new ErrorSubscribeException(UPDATE, QUERY
                            , "Update result[sequenceId(based one):%s] expect subscribe nextUpdate() but subscribe nextQuery()");
                    addDownstreamError(e);
                    realSink.error(e);
                }
                bufferSink.clearBuffer();
            } else if (bufferSink instanceof BufferQuerySink) {
                if (realSink != null) {
                    ErrorSubscribeException e = new ErrorSubscribeException(QUERY, UPDATE
                            , "Query result[sequenceId(based one):%s] expect subscribe nextQuery() but subscribe nextUpdate()");
                    addDownstreamError(e);
                    realSink.error(e);
                }
                bufferSink.clearBuffer();
            } else {
                throw new IllegalStateException(String.format("Unknown %s type.", bufferSink));
            }
        }

        if (!JdbdCollections.isEmpty(this.downstreamErrorList)) {
            emitErrorToSinkQueue(createException());
        }
    }

    private JdbdException createException() {
        JdbdException upstreamError = this.upstreamError;
        List<JdbdException> errorList = this.downstreamErrorList;
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
            if (MultiResultCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            } else {
                throw new IllegalStateException(
                        String.format("%s not complete,reject next update result[sequenceId:%s]."
                                , this, sequenceId));
            }
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) throws IllegalStateException {
            if (MultiResultCreate.this.currentSink != this) {
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
            if (!MultiResultCreate.this.hasError()) {
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
         * @see MultiResultCreate#drainReceiver()
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
         * @see MultiResultCreate#subscribeNextQuery(FluxSink, Consumer)
         */
        private void subscribe(final FluxSink<ResultRow> sink, final Consumer<ResultStates> statesConsumer) {
            if (MultiResultCreate.this.currentSink != this) {
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
            MultiResultCreate.this.updateLastResultStates(resultStates);

            Consumer<ResultStates> statesConsumer = this.statesConsumer;

            if (statesConsumer != null) {
                statesConsumer.accept(resultStates);
            }
        }


        @Override
        public void complete() {
            if (MultiResultCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.resultStates == null) {
                throw new IllegalStateException(
                        String.format("%s Can't complete before invoke %s.accept(ResultStates resultStates)"
                                , this, QuerySink.class.getName()));
            }
            // firstly
            MultiResultCreate.this.currentSink = null;

            FluxSink<ResultRow> actualSink = this.actualSink;
            if (actualSink == null) {
                MultiResultCreate.this.addBufferDownstreamSink(this);
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
            if (MultiResultCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.sequenceId > 0) {
                throw new IllegalStateException(String.format("%s not complete,reject next update result[sequenceId:%s]"
                        , this, this.sequenceId));
            }
            this.sequenceId = sequenceId;
            MultiResultCreate.this.currentSink = null;
            ErrorSubscribeException e = new ErrorSubscribeException(QUERY, UPDATE
                    , "Query result[sequenceId(based one):%s] expect subscribe nextQuery ,but subscribe nextUpdate."
                    , sequenceId);

            addDownstreamError(e);

            this.sink.error(e);
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) throws IllegalStateException {
            if (MultiResultCreate.this.currentSink != this) {
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
        public final void nextUpdate(final int sequenceId, ResultStates resultStates) {
            if (MultiResultCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            this.sequenceId = sequenceId;
            MultiResultCreate.this.currentSink = null;
            this.sink.success(resultStates);
        }

        final void fromBuffer(BufferUpdateSink buffer) {
            this.sink.success(Objects.requireNonNull(buffer.resultStates, "buffer.resultStates"));
        }

        @Override
        public QuerySink nextQuery(final int sequenceId) {
            if (MultiResultCreate.this.currentSink != this) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }
            if (this.sequenceId > 0) {
                throw new IllegalStateException(String.format("%s not complete,reject next query result[sequenceId:%s]"
                        , this, this.sequenceId));
            }
            this.sequenceId = sequenceId;
            // here, downstream subscribe error,should subscribe io.jdbd.result.MultiResults.nextUpdate.
            ErrorSubscribeException e = new ErrorSubscribeException(UPDATE, QUERY
                    , "Result[sequenceId(based one):%s] Expect subscribe nextQuery,but subscribe nextUpdate."
                    , sequenceId);

            MultiResultCreate.this.addDownstreamError(e);
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
            if (!MultiResultCreate.this.hasDownstreamError()) {
                this.actualSink.next(resultRow);
            }
        }


        @Override
        public void complete() throws IllegalStateException {
            final DownstreamSink currentSink = MultiResultCreate.this.currentSink;
            if (currentSink != this.downstreamSink) {
                throw new IllegalStateException(String.format("%s have ended.", this));
            }

            if (this.resultStates == null) {
                throw new IllegalStateException(
                        String.format("%s Can't complete before invoke %s.accept(ResultStates resultStates)"
                                , this, QuerySink.class.getName()));
            }
            // firstly
            MultiResultCreate.this.currentSink = null;
            // secondly
            if (MultiResultCreate.this.hasError()) {
                final Throwable e = MultiResultCreate.this.createException();
                if (this.downstreamSink instanceof RealUpdateSink) {
                    if (this.actualSink instanceof DirtyFluxSink) {
                        ((RealUpdateSink) this.downstreamSink).sink.error(e);
                    } else {
                        this.actualSink.error(e);
                    }
                } else {
                    this.actualSink.error(e);
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
            MultiResultCreate.this.updateLastResultStates(resultStates);
            if (!MultiResultCreate.this.hasError()) {
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


    private static final class DefaultMultiResult implements ReactorMultiResult {

        private final MultiResultCreate resultsSink;

        private DefaultMultiResult(MultiResultCreate resultsSink) {
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
