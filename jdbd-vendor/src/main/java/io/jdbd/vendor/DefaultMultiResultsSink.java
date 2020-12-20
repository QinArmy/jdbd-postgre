package io.jdbd.vendor;

import io.jdbd.*;
import io.jdbd.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public final class DefaultMultiResultsSink implements MultiResultsSink {


    public static DefaultMultiResultsSink forTask(AbstractCommunicationTask task, int expectedResultCount) {
        return new DefaultMultiResultsSink(task, expectedResultCount);
    }


    private static final Logger LOG = LoggerFactory.getLogger(DefaultMultiResultsSink.class);


    private final AbstractCommunicationTask task;

    private final MultiResults multiResults = new MultiResultsImpl(this);

    private final int expectedResultCount;

    private int receiveResultCount = 0;

    private Queue<DownstreamSubscriber> downstreamSubscriberQueue;

    private ResultSubscriber currentSubscriber;

    /**
     * record {@link #currentSubscriber} when satisfy below all:
     * <ul>
     *     <li>{@link #currentSubscriber} is {@link DownstreamSubscriber}</li>
     *     <li>{@link #currentSubscriber} will set to {@link TooManyResultBufferSubscriber#INSTANCE}</li>
     * </ul>
     */
    private DownstreamSubscriber pendingSubscriber;

    private Queue<BufferSubscriber> bufferSubscriberQueue;


    private DefaultMultiResultsSink(AbstractCommunicationTask task, int expectedResultCount) {
        this.task = task;
        this.expectedResultCount = expectedResultCount;
    }


    public final MultiResults getMultiResults() {
        return this.multiResults;
    }


    /**
     * <p>
     *     <ol>
     *         <li>if {@link #currentSubscriber} is {@link DownstreamSubscriber},emit {@link JdbdSQLException} to downstream</li>
     *         <li>set {@link #currentSubscriber} as {@link AccessExceptionBufferSubscriber},and don't update again</li>
     *     </ol>
     * </p>
     * <p>
     *     note:after invoke this method, this {@link CommunicationTask} must end.
     * </p>
     *
     * @param e database return exception
     * @throws IllegalStateException throw when {@link #currentSubscriber} is {@link AccessExceptionBufferSubscriber}.
     * @see AccessExceptionBufferSubscriber
     */
    public final void error(Throwable e) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;

        final AccessExceptionBufferSubscriber ae;
        if (currentSubscriber instanceof TooManyResultBufferSubscriber) {
            String tooManyMessage = buildTooManyMessageWhenAccessError();
            // 1. firstly, set currentSubscriber to AccessExceptionBufferSubscriber, avoid to downstream update currentSubscriber
            ae = new AccessExceptionBufferSubscriber(e, tooManyMessage);
            this.currentSubscriber = ae;

            // 2. bow handle currentSubscriber
            final DownstreamSubscriber pendingSubscriber = this.pendingSubscriber;
            if (pendingSubscriber instanceof FluxDownstreamSubscriber) {
                this.pendingSubscriber = null;
                ((FluxDownstreamSubscriber) pendingSubscriber).sink.error(convertError(e, tooManyMessage));
            } else if (pendingSubscriber instanceof MonoDownstreamSubscriber) {
                this.pendingSubscriber = null;
                ((MonoDownstreamSubscriber) pendingSubscriber).sink.error(convertError(e, tooManyMessage));
            } else if (pendingSubscriber != null) {
                throw createNonExpectedResultSubscriberTypeError(pendingSubscriber);
            }
        } else {
            // 1. firstly, set currentSubscriber to AccessExceptionBufferSubscriber, avoid to downstream update currentSubscriber
            ae = new AccessExceptionBufferSubscriber(e);
            this.currentSubscriber = ae;

            // 2. bow handle currentSubscriber
            if (currentSubscriber == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("emit access error,currentSubscriber is null.");
                }
            } else if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
                throw new IllegalStateException(String.format("currentSubscriber is %s ,access error duplication."
                        , AccessExceptionBufferSubscriber.class.getName()));
            } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) currentSubscriber).sink.error(convertError(e, null));
            } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) currentSubscriber).sink.error(convertError(e, null));
            } else if (currentSubscriber instanceof FluxBufferSubscriber) {
                ((FluxBufferSubscriber) currentSubscriber).resultRowQueue.clear();
            } else {
                throw createNonExpectedResultSubscriberTypeError(currentSubscriber);
            }

        }

        // 3. emit AccessExceptionBufferSubscriber to downstream .
        final Queue<DownstreamSubscriber> subscriberQueue = this.downstreamSubscriberQueue;
        if (subscriberQueue != null && !subscriberQueue.isEmpty()) {
            DownstreamSubscriber downstreamSubscriber;
            while ((downstreamSubscriber = subscriberQueue.poll()) != null) {
                if (downstreamSubscriber instanceof FluxDownstreamSubscriber) {
                    ((FluxDownstreamSubscriber) downstreamSubscriber).sink.error(convertError(ae.accessError, ae.message));
                } else if (downstreamSubscriber instanceof MonoDownstreamSubscriber) {
                    ((MonoDownstreamSubscriber) downstreamSubscriber).sink.error(convertError(ae.accessError, ae.message));
                } else {
                    throw createNonExpectedResultSubscriberTypeError(downstreamSubscriber);
                }
            }
        }


    }


    public final void nextUpdate(ResultStates resultStates, final boolean hasMore) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessExceptionBufferSubscriber) currentSubscriber);
        }
        this.currentSubscriber = null;
        this.receiveResultCount++;
        if (currentSubscriber instanceof TooManyResultBufferSubscriber && hasMore) {
            getOrCreateBufferSubscriber().add((TooManyResultBufferSubscriber) currentSubscriber);
        } else if (currentSubscriber instanceof TooManyResultBufferSubscriber) {
            final DownstreamSubscriber pending = this.pendingSubscriber;
            this.pendingSubscriber = null;
            if (pending instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) pending).sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else if (pending instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) pending).sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else if (pending != null) {
                throw createNonExpectedResultSubscriberTypeError(pending);
            }
        } else if (this.receiveResultCount == this.expectedResultCount && hasMore) {
            this.currentSubscriber = TooManyResultBufferSubscriber.INSTANCE;
            if (currentSubscriber == null) {
                getOrCreateBufferSubscriber().add(TooManyResultBufferSubscriber.INSTANCE);
            } else if (currentSubscriber instanceof MonoDownstreamSubscriber
                    || currentSubscriber instanceof FluxDownstreamSubscriber) {
                this.pendingSubscriber = (DownstreamSubscriber) currentSubscriber;
            } else {
                throw createNonExpectedResultSubscriberTypeError(currentSubscriber);
            }
        } else if (currentSubscriber == null) {
            getOrCreateBufferSubscriber().add(new MonoBufferSubscriber(resultStates));
        } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
            ((MonoDownstreamSubscriber) currentSubscriber).sink.success(resultStates);
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            ((FluxDownstreamSubscriber) currentSubscriber).sink.error(SubscriptionNotMatchException.expectUpdate());
        } else {
            throw createNonExpectedResultSubscriberTypeError(currentSubscriber);
        }

        publishHeapUpBufferIfNeed();

    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #obtainCurrentRowSink()
     * @see #emitRowTerminator(ResultStates, boolean)
     */
    public final void nextQueryRowMeta(ResultRowMeta rowMeta) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        if (currentSubscriber == null) {
            this.currentSubscriber = new FluxBufferSubscriber(rowMeta);
        } else if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessExceptionBufferSubscriber) currentSubscriber);
        } else if (currentSubscriber instanceof TooManyResultBufferSubscriber) {
            this.currentSubscriber = new FluxTooManyResultBufferSubscriber(rowMeta);
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            FluxDownstreamSubscriber downstreamSubscriber = (FluxDownstreamSubscriber) currentSubscriber;
            if (downstreamSubscriber.rowMeta != null) {
                throw new IllegalStateException(String.format("%s.rowMeta not null"
                        , FluxDownstreamSubscriber.class.getName()));
            }
            downstreamSubscriber.rowMeta = rowMeta;
        } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
            this.currentSubscriber = new FluxNotMatchSubscriber(rowMeta);
            ((MonoDownstreamSubscriber) currentSubscriber).sink.error(SubscriptionNotMatchException.expectQuery()); // emit error.
        } else {
            throw new IllegalStateException(String.format("currentSubscriber isn' expected type[%s]."
                    , currentSubscriber.getClass().getName()));
        }
    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #nextQueryRowMeta(ResultRowMeta)
     * @see #emitRowTerminator(ResultStates, boolean)
     */
    public final MultiResultsSink.RowSink obtainCurrentRowSink() {
        final ResultSubscriber currentSubscriber = Objects.requireNonNull(this.currentSubscriber, "currentSubscriber");
        final RowSink sink;
        if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessExceptionBufferSubscriber) currentSubscriber);
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            sink = ((FluxDownstreamSubscriber) currentSubscriber).rowSink;
        } else if (currentSubscriber instanceof FluxTooManyResultBufferSubscriber) {
            sink = ((FluxTooManyResultBufferSubscriber) currentSubscriber).sink;
        } else if (currentSubscriber instanceof FluxBufferSubscriber) {
            sink = ((FluxBufferSubscriber) currentSubscriber).sink;
        } else if (currentSubscriber instanceof FluxNotMatchSubscriber) {
            sink = ((FluxNotMatchSubscriber) currentSubscriber).sink;
        } else {
            throw createNonExpectedResultSubscriberTypeError(currentSubscriber);
        }
        return sink;
    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #nextQueryRowMeta(ResultRowMeta)
     * @see #obtainCurrentRowSink()
     */
    public final void emitRowTerminator(ResultStates states, final boolean hasMore) {
        final ResultSubscriber currentSubscriber = Objects.requireNonNull(this.currentSubscriber, "currentSubscriber");
        if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessExceptionBufferSubscriber) this.currentSubscriber);
        }
        this.currentSubscriber = null; // clear for downstream
        final boolean tooManyResultError = (++this.receiveResultCount) == this.expectedResultCount && hasMore;

        if (tooManyResultError) {
            this.currentSubscriber = TooManyResultBufferSubscriber.INSTANCE; // don't accept subscribe again.
        }
        if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            // publish to downstream
            final FluxDownstreamSubscriber downstreamSubscriber = (FluxDownstreamSubscriber) currentSubscriber;
            if (tooManyResultError) {
                downstreamSubscriber.sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else {
                boolean canComplete = false;
                try {
                    if (!downstreamSubscriber.rowSink.rowDecoderError) {
                        downstreamSubscriber.statesConsumer.accept(states);
                        canComplete = !downstreamSubscriber.sink.isCancelled();
                    }
                } catch (Throwable e) {
                    downstreamSubscriber.sink.error(new ResultStateConsumerException(
                            "stateConsumer throw exception", e));
                }
                if (canComplete) {
                    downstreamSubscriber.sink.complete();
                }
            }

        } else if (currentSubscriber instanceof FluxBufferSubscriber) {
            FluxBufferSubscriber bufferSubscriber = (FluxBufferSubscriber) currentSubscriber;
            if (tooManyResultError) {
                bufferSubscriber.tooManyResultError = true;
            } else {
                if (bufferSubscriber.resultStates != null) {
                    throw new IllegalStateException(String.format("%s.resultStates isn't null."
                            , FluxBufferSubscriber.class.getName()));
                }
                bufferSubscriber.resultStates = states;
                getOrCreateBufferSubscriber().add(bufferSubscriber); // add to queue or publish to downstream
            }
        } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
            MonoDownstreamSubscriber downstreamSubscriber = (MonoDownstreamSubscriber) currentSubscriber;
            if (tooManyResultError) {
                downstreamSubscriber.sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else {
                downstreamSubscriber.sink.error(SubscriptionNotMatchException.expectQuery());
            }
        } else if (!(currentSubscriber instanceof FluxTooManyResultBufferSubscriber)) {
            throw createNonExpectedResultSubscriberTypeError(currentSubscriber);
        }

    }

    /*################################## blow private method ##################################*/

    private Queue<BufferSubscriber> getOrCreateBufferSubscriber() {
        Queue<BufferSubscriber> bufferSubscriberQueue = this.bufferSubscriberQueue;
        if (bufferSubscriberQueue == null) {
            bufferSubscriberQueue = createQueue();
            this.bufferSubscriberQueue = bufferSubscriberQueue;
        }
        return bufferSubscriberQueue;
    }


    private String buildTooManyMessageWhenAccessError() {
        return String.format(
                "database access error,but before occur %s,expect %s,but at least receive %s ."
                , TooManyResultException.class.getName(), this.expectedResultCount, this.receiveResultCount);
    }


    private void publishHeapUpBufferIfNeed() {
        final Queue<DownstreamSubscriber> subscriberQueue = this.downstreamSubscriberQueue;
        final Queue<BufferSubscriber> bufferQueue = this.bufferSubscriberQueue;

        if (subscriberQueue != null && bufferQueue != null) {
            while (!bufferQueue.isEmpty() && !subscriberQueue.isEmpty()) {
                publishBuffer(bufferQueue.poll(), subscriberQueue.poll());
            }
        }
    }


    private void publishBuffer(BufferSubscriber buffer, DownstreamSubscriber subscriber) {
        if (buffer instanceof AccessExceptionBufferSubscriber) {
            AccessExceptionBufferSubscriber aes = ((AccessExceptionBufferSubscriber) buffer);
            Throwable e = convertError(aes.accessError, aes.message);
            if (subscriber instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) subscriber).sink.error(e);
            } else if (subscriber instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) subscriber).sink.error(e);
            } else {
                throw createNonExpectedResultSubscriberTypeError(subscriber);
            }
        } else if (buffer instanceof MonoBufferSubscriber && subscriber instanceof MonoDownstreamSubscriber) {
            ((MonoDownstreamSubscriber) subscriber).sink.success(((MonoBufferSubscriber) buffer).resultStates);
        } else if (buffer instanceof FluxBufferSubscriber && subscriber instanceof FluxDownstreamSubscriber) {
            publishFluxBuffer((FluxBufferSubscriber) buffer, (FluxDownstreamSubscriber) subscriber);
        } else {
            publishSubscriptionNotMatchError(buffer, subscriber);
        }
    }

    /**
     * @see #publishHeapUpBufferIfNeed()
     */
    private void publishFluxBuffer(FluxBufferSubscriber buffer, FluxDownstreamSubscriber subscriber) {
        final FluxSink<Object> downstreamSink = subscriber.sink;
        final ResultStates resultStates = buffer.resultStates;
        if (resultStates == null) {
            downstreamSink.error(new NullPointerException(
                    "jdbd implementation bug,FluxBufferSubscriber.resultStates is null."));
            return;
        }

        final Queue<ResultRow> rowQueue = buffer.resultRowQueue;

        ResultRow resultRow;
        while ((resultRow = rowQueue.poll()) != null) {
            Object rowValue;
            try {
                rowValue = subscriber.rowDecoder.apply(resultRow, subscriber.rowMeta);
            } catch (Throwable e) {
                downstreamSink.error(new RowDecoderException("rowDecoder throw exception.", e));
                return;
            }
            downstreamSink.next(rowValue);
        }

        Throwable databaseError = buffer.accessError;
        if (databaseError == null) {
            try {
                subscriber.statesConsumer.accept(resultStates);
                downstreamSink.complete();
            } catch (Throwable e) {
                downstreamSink.error(new ResultStateConsumerException("stateConsumer throw exception", e));
            }
        } else {
            downstreamSink.error(databaseError);
        }

    }

    /**
     * @see #publishHeapUpBufferIfNeed()
     */
    private void publishSubscriptionNotMatchError(BufferSubscriber buffer, DownstreamSubscriber subscriber) {
        if (buffer instanceof FluxBufferSubscriber && subscriber instanceof MonoDownstreamSubscriber) {
            ((MonoDownstreamSubscriber) subscriber).sink.error(SubscriptionNotMatchException.expectQuery());
        } else if (buffer instanceof MonoBufferSubscriber && subscriber instanceof FluxDownstreamSubscriber) {
            ((FluxDownstreamSubscriber) subscriber).sink.error(SubscriptionNotMatchException.expectUpdate());
        } else {
            throw createNonExpectedResultSubscriberTypeError(subscriber);
        }
    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     */
    private void addMonoSubscriber(MonoSink<ResultStates> sink) {
        if (subscribePermit(sink::error)) {
            final MonoDownstreamSubscriber downstreamSubscriber = new MonoDownstreamSubscriber(sink);
            addDownstreamSubscriber(downstreamSubscriber, sink::error);
        }
    }

    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     */
    private void addFluxSubscriber(FluxSink<?> sink, BiFunction<ResultRow, ResultRowMeta, ?> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        if (subscribePermit(sink::error)) {
            @SuppressWarnings("unchecked")
            FluxSink<Object> objectSink = (FluxSink<Object>) sink;
            final FluxDownstreamSubscriber downstreamSubscriber = new FluxDownstreamSubscriber(
                    objectSink, rowDecoder, statesConsumer);

            addDownstreamSubscriber(downstreamSubscriber, sink::error);
        }

    }

    private void addDownstreamSubscriber(DownstreamSubscriber downstreamSubscriber
            , Consumer<TaskQueueOverflowException> sinkError) {
        Queue<DownstreamSubscriber> queue = this.downstreamSubscriberQueue;
        if (queue != null) {
            queue.add(downstreamSubscriber);
        } else if (this.currentSubscriber == null) {
            this.currentSubscriber = downstreamSubscriber;
        } else {
            queue = createQueue();
            this.downstreamSubscriberQueue = queue;
            queue.add(downstreamSubscriber);
        }
        if (this.task.getTaskPhase() == null) {
            this.task.submit(sinkError);  // submit task
        } else {
            publishHeapUpBufferIfNeed();
        }
    }

    private boolean subscribePermit(Consumer<Throwable> sink) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        final boolean permit;
        if (this.task.getTaskPhase() == CommunicationTask.TaskPhase.END
                && (this.bufferSubscriberQueue == null || this.bufferSubscriberQueue.isEmpty())) {
            sink.accept(new NoMoreResultException(String.format("%s no more result,cannot subscribe.", MultiResults.class.getName())));
            permit = false;
        } else if (currentSubscriber instanceof AccessExceptionBufferSubscriber) {
            AccessExceptionBufferSubscriber ae = (AccessExceptionBufferSubscriber) currentSubscriber;
            sink.accept(convertError(ae.accessError, ae.message));
            permit = false;
        } else if (currentSubscriber instanceof TooManyResultBufferSubscriber) {
            sink.accept(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            permit = false;
        } else {
            permit = true;
        }
        return permit;
    }

    private Throwable convertError(Throwable e, @Nullable String message) {
        Throwable ex;
        if (e instanceof JdbdSQLException) {
            ex = e;
        } else if (e instanceof SQLException) {
            if (message == null) {
                ex = new JdbdSQLException((SQLException) e);
            } else {
                ex = new JdbdSQLException(message, (SQLException) e);
            }
        } else {
            if (message == null) {
                ex = new JdbdNonSQLException(e.getMessage(), e) {
                };
            } else {
                ex = new JdbdNonSQLException(message, e) {
                };
            }
        }
        return ex;
    }



    /*################################## blow private static method ##################################*/

    private static <T> Queue<T> createQueue() {
        return new LinkedList<>();
    }

    private static IllegalStateException createTaskEndWithDatabaseAccessError(AccessExceptionBufferSubscriber error) {
        String m = String.format("%s have ended with database access error :%s."
                , CommunicationTask.class.getName()
                , error.accessError.getMessage());
        return new IllegalStateException(m);
    }


    private static IllegalStateException createNonExpectedResultSubscriberTypeError(ResultSubscriber subscriber) {
        return new IllegalStateException(String.format("Unknown %s type[%s]"
                , ResultSubscriber.class.getName(), subscriber.getClass().getName()));
    }




    /*################################## blow protected  static class ##################################*/




    /*################################## blow private static inner class  ##################################*/

    private static final class MultiResultsImpl implements MultiResults {

        private final DefaultMultiResultsSink resultsSink;

        private MultiResultsImpl(DefaultMultiResultsSink resultsSink) {
            this.resultsSink = resultsSink;
        }

        @Override
        public final Mono<ResultStates> nextUpdate() {
            return Mono.create(sink -> {
                if (this.resultsSink.task.executorAdjutant.inEventLoop()) {
                    this.resultsSink.addMonoSubscriber(sink);
                } else {
                    this.resultsSink.task.executorAdjutant.execute(() -> this.resultsSink.addMonoSubscriber(sink));
                }
            });
        }

        @Override
        public final <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
                , Consumer<ResultStates> statesConsumer) {
            return Flux.create(sink -> {
                if (this.resultsSink.task.executorAdjutant.inEventLoop()) {
                    this.resultsSink.addFluxSubscriber(sink, rowDecoder, statesConsumer);
                } else {
                    this.resultsSink.task.executorAdjutant.execute(
                            () -> this.resultsSink.addFluxSubscriber(sink, rowDecoder, statesConsumer));
                }
            });
        }
    }

    private interface ResultSubscriber {

    }

    private interface BufferSubscriber extends ResultSubscriber {


    }

    private interface DownstreamSubscriber extends ResultSubscriber {

    }

    private static class AccessExceptionBufferSubscriber implements BufferSubscriber {

        final Throwable accessError;

        final String message;


        AccessExceptionBufferSubscriber(Throwable accessError, String message) {
            this.accessError = accessError;
            this.message = message;
        }

        AccessExceptionBufferSubscriber(Throwable accessError) {
            this.accessError = accessError;
            this.message = "Database access error.";
        }
    }


    protected static final class MonoDownstreamSubscriber implements DownstreamSubscriber {

        private final MonoSink<ResultStates> sink;

        private MonoDownstreamSubscriber(MonoSink<ResultStates> sink) {
            this.sink = sink;
        }
    }

    protected static final class FluxDownstreamSubscriber implements DownstreamSubscriber {

        private final Consumer<ResultStates> statesConsumer;

        private final FluxSink<Object> sink;

        private final BiFunction<ResultRow, ResultRowMeta, ?> rowDecoder;

        private final DownstreamRowSink rowSink = new DownstreamRowSink(this);

        //non-volatile ,modify in netty EventLoop.
        private ResultRowMeta rowMeta;

        private FluxDownstreamSubscriber(FluxSink<Object> sink
                , BiFunction<ResultRow, ResultRowMeta, ?> rowDecoder, Consumer<ResultStates> statesConsumer) {
            this.sink = sink;
            this.rowDecoder = rowDecoder;
            this.statesConsumer = statesConsumer;
        }


    }


    private static class MonoBufferSubscriber implements BufferSubscriber {

        private final ResultStates resultStates;

        private MonoBufferSubscriber(ResultStates resultStates) {
            this.resultStates = resultStates;
        }

    }

    private static class TooManyResultBufferSubscriber implements BufferSubscriber {
        private static final TooManyResultBufferSubscriber INSTANCE = new TooManyResultBufferSubscriber();

        private TooManyResultBufferSubscriber() {
        }
    }

    private static final class FluxTooManyResultBufferSubscriber extends TooManyResultBufferSubscriber {

        private final RowSink sink;

        private FluxTooManyResultBufferSubscriber(ResultRowMeta rowMeta) {
            this.sink = EmptyRowSink.create(rowMeta);
        }
    }

    private static final class FluxNotMatchSubscriber implements BufferSubscriber {

        private final RowSink sink;

        private FluxNotMatchSubscriber(ResultRowMeta rowMeta) {
            this.sink = EmptyRowSink.create(rowMeta);
        }
    }


    private static final class FluxBufferSubscriber implements BufferSubscriber {

        private final ResultRowMeta rowMeta;

        private final Queue<ResultRow> resultRowQueue = createQueue();

        private final RowSink sink = new BufferRowSink(this);

        //non-volatile ,modify in netty EventLoop.
        private ResultStates resultStates;

        //non-volatile ,modify in netty EventLoop.
        private SQLException accessError;

        private boolean tooManyResultError;


        private FluxBufferSubscriber(ResultRowMeta rowMeta) {
            this.rowMeta = rowMeta;
        }
    }


    private static final class BufferRowSink implements RowSink {

        private final FluxBufferSubscriber subscriber;

        private BufferRowSink(FluxBufferSubscriber subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void error(SQLException e) {
            if (this.subscriber.accessError != null) {
                throw new IllegalStateException("Access database error duplication.");
            }
            this.subscriber.accessError = e;
        }

        @Override
        public ResultRowMeta getRowMeta() {
            return this.subscriber.rowMeta;
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public void next(ResultRow row) {
            this.subscriber.resultRowQueue.add(row);
        }
    }


    private static final class DownstreamRowSink implements RowSink {

        private final FluxDownstreamSubscriber subscriber;

        //non-volatile ,modify in netty EventLoop.
        private boolean rowDecoderError;

        private DownstreamRowSink(FluxDownstreamSubscriber subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void error(SQLException e) {
            this.subscriber.sink.error(new JdbdSQLException("Access database occur error.", e));
        }

        @Override
        public ResultRowMeta getRowMeta() {
            return Objects.requireNonNull(this.subscriber.rowMeta, "this.subscriber.rowMeta");
        }

        @Override
        public boolean isCanceled() {
            return this.rowDecoderError || this.subscriber.sink.isCancelled();
        }

        @Override
        public void next(ResultRow resultRow) {
            if (this.rowDecoderError) {
                return;
            }
            Object rowValue = null;
            try {
                rowValue = subscriber.rowDecoder.apply(resultRow, subscriber.rowMeta);
            } catch (Throwable e) {
                this.subscriber.sink.error(new RowDecoderException("rowDecoder throw exception.", e));
                this.rowDecoderError = true;
            }
            if (rowValue == null) {
                this.rowDecoderError = true;
                this.subscriber.sink.error(new NullPointerException("rowDecoder return null."));
            } else {
                this.subscriber.sink.next(rowValue);
            }
        }
    }


}
