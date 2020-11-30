package io.jdbd.vendor;

import io.jdbd.*;
import io.netty.buffer.ByteBuf;
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

public abstract class AbstractSQLCommTask extends AbstractCommTask implements ReactorMultiResults {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSQLCommTask.class);

    private static final RowSink EMPTY_ROW_SINK = new RowSink() {
        @Override
        public boolean isCanceled() {
            return true;
        }

        @Override
        public void next(ResultRow resultRow) {
            // no-op
        }
    };

    private static final BufferSubscriber TOO_MANY_RESULT_ERROR_SUBSCRIBER = new BufferSubscriber() {
    };


    private final int expectedResultCount;

    private int receiveResultCount = 0;

    private Queue<DownstreamSubscriber> downstreamSubscriberQueue;

    private ResultSubscriber currentSubscriber;

    /**
     * record {@link #currentSubscriber} when satisfy below all:
     * <ul>
     *     <li>{@link #currentSubscriber} is {@link DownstreamSubscriber}</li>
     *     <li>{@link #currentSubscriber} will set to {@link #TOO_MANY_RESULT_ERROR_SUBSCRIBER}</li>
     * </ul>
     */
    private DownstreamSubscriber pendingSubscriber;

    private Queue<BufferSubscriber> bufferSubscriberQueue;


    protected AbstractSQLCommTask(CommTaskExecutorAdjutant executorAdjutant, int expectedResultCount) {
        super(executorAdjutant);
        this.expectedResultCount = expectedResultCount;
    }

    @Override
    public final Mono<ResultStates> nextUpdate() {
        return Mono.create(sink -> {
            if (this.executorAdjutant.inEventLoop()) {
                addMonoSubscriber(sink);
            } else {
                this.executorAdjutant.execute(() -> addMonoSubscriber(sink));
            }
        });
    }

    @Override
    public final <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        return Flux.create(sink -> {
            if (this.executorAdjutant.inEventLoop()) {
                addFluxSubscriber(sink, rowDecoder, statesConsumer);
            } else {
                this.executorAdjutant.execute(() -> addFluxSubscriber(sink, rowDecoder, statesConsumer));
            }
        });
    }


    /**
     * <p>
     *     <ol>
     *         <li>if {@link #currentSubscriber} is {@link DownstreamSubscriber},emit {@link JdbdSQLException} to downstream</li>
     *         <li>set {@link #currentSubscriber} as {@link AccessErrorBufferSubscriber},and don't update again</li>
     *     </ol>
     * </p>
     * <p>
     *     note:after invoke this method, this {@link CommunicationTask} must end.
     * </p>
     *
     * @param e database return exception
     * @throws IllegalStateException throw when {@link #currentSubscriber} is {@link AccessErrorBufferSubscriber}.
     * @see AccessErrorBufferSubscriber
     */
    protected final void emitDatabaseAccessError(SQLException e) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;

        final AccessErrorBufferSubscriber ae;
        if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER) {
            String tooManyMessage = buildTooManyMessageWhenAccessError();
            // 1. firstly, set currentSubscriber to AccessErrorBufferSubscriber, avoid to downstream update currentSubscriber
            ae = new AccessErrorBufferSubscriber(e, tooManyMessage);
            this.currentSubscriber = ae;

            // 2. bow handle currentSubscriber
            final DownstreamSubscriber pendingSubscriber = this.pendingSubscriber;
            if (pendingSubscriber instanceof FluxDownstreamSubscriber) {
                this.pendingSubscriber = null;
                ((FluxDownstreamSubscriber) pendingSubscriber).sink.error(new JdbdSQLException(tooManyMessage, e));
            } else if (pendingSubscriber instanceof MonoDownstreamSubscriber) {
                this.pendingSubscriber = null;
                ((MonoDownstreamSubscriber) pendingSubscriber).sink.error(new JdbdSQLException(tooManyMessage, e));
            } else if (pendingSubscriber != null) {
                throw createUnknownResultSubscriberTypeError(pendingSubscriber);
            }
        } else {
            // 1. firstly, set currentSubscriber to AccessErrorBufferSubscriber, avoid to downstream update currentSubscriber
            ae = new AccessErrorBufferSubscriber(e);
            this.currentSubscriber = ae;

            // 2. bow handle currentSubscriber
            if (currentSubscriber == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("emit access error,currentSubscriber is null.");
                }
            } else if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
                throw new IllegalStateException(String.format("currentSubscriber is %s ,access error duplication."
                        , AccessErrorBufferSubscriber.class.getName()));
            } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) currentSubscriber).sink.error(new JdbdSQLException(e));
            } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) currentSubscriber).sink.error(new JdbdSQLException(e));
            } else if (currentSubscriber instanceof FluxBufferSubscriber) {
                ((FluxBufferSubscriber) currentSubscriber).resultRowQueue.clear();
            } else {
                throw createUnknownResultSubscriberTypeError(currentSubscriber);
            }

        }

        // 3. emit AccessErrorBufferSubscriber to downstream .
        final Queue<DownstreamSubscriber> subscriberQueue = this.downstreamSubscriberQueue;
        if (subscriberQueue != null && !subscriberQueue.isEmpty()) {
            DownstreamSubscriber downstreamSubscriber;
            while ((downstreamSubscriber = subscriberQueue.poll()) != null) {
                if (downstreamSubscriber instanceof FluxDownstreamSubscriber) {
                    ((FluxDownstreamSubscriber) downstreamSubscriber).sink.error(new JdbdSQLException(ae.message, ae.accessError));
                } else if (downstreamSubscriber instanceof MonoDownstreamSubscriber) {
                    ((MonoDownstreamSubscriber) downstreamSubscriber).sink.error(new JdbdSQLException(ae.message, ae.accessError));
                } else {
                    throw createUnknownResultSubscriberTypeError(downstreamSubscriber);
                }
            }
        }


    }


    protected final void emitUpdateResult(ResultStates resultStates, final boolean hasMore) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessErrorBufferSubscriber) currentSubscriber);
        }
        this.currentSubscriber = null;
        this.receiveResultCount++;
        if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER && hasMore) {
            getOrCreateBufferSubscriber().add(TOO_MANY_RESULT_ERROR_SUBSCRIBER);
        } else if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER) {
            final DownstreamSubscriber pending = this.pendingSubscriber;
            this.pendingSubscriber = null;
            if (pending instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) pending).sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else if (pending instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) pending).sink.error(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            } else if (pending != null) {
                throw createUnknownResultSubscriberTypeError(pending);
            }
        } else if (this.receiveResultCount == this.expectedResultCount && hasMore) {
            this.currentSubscriber = TOO_MANY_RESULT_ERROR_SUBSCRIBER;
            if (currentSubscriber == null) {
                getOrCreateBufferSubscriber().add(TOO_MANY_RESULT_ERROR_SUBSCRIBER);
            } else if (currentSubscriber instanceof MonoDownstreamSubscriber
                    || currentSubscriber instanceof FluxDownstreamSubscriber) {
                this.pendingSubscriber = (DownstreamSubscriber) currentSubscriber;
            } else {
                throw createUnknownResultSubscriberTypeError(currentSubscriber);
            }
        } else if (currentSubscriber == null) {
            getOrCreateBufferSubscriber().add(new MonoBufferSubscriber(resultStates));
        } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
            ((MonoDownstreamSubscriber) currentSubscriber).sink.success(resultStates);
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            ((FluxDownstreamSubscriber) currentSubscriber).sink.error(SubscriptionNotMatchException.expectUpdate());
        } else {
            throw createUnknownResultSubscriberTypeError(currentSubscriber);
        }

        publishHeapUpBufferIfNeed();

    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #decodeMultiRowData(ByteBuf)
     * @see #emitResultRowTerminator(ResultStates, boolean)
     */
    protected final void setCurrentSubscriberRowMeta(ResultRowMeta rowMeta) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        if (currentSubscriber == null) {
            this.currentSubscriber = new FluxBufferSubscriber(rowMeta);
        } else if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessErrorBufferSubscriber) currentSubscriber);
        } else if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("currentSubscriber is TOO_MANY_RESULT_ERROR_SUBSCRIBER");
            }
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            FluxDownstreamSubscriber downstreamSubscriber = (FluxDownstreamSubscriber) currentSubscriber;
            if (downstreamSubscriber.rowMeta != null) {
                throw new IllegalStateException(String.format("%s.rowMeta not null"
                        , FluxDownstreamSubscriber.class.getName()));
            }
            downstreamSubscriber.rowMeta = Objects.requireNonNull(rowMeta, "parma rowMeta");
        } else if (currentSubscriber instanceof MonoDownstreamSubscriber) {
            MonoDownstreamSubscriber downstreamSubscriber = (MonoDownstreamSubscriber) currentSubscriber;
            downstreamSubscriber.sink.error(SubscriptionNotMatchException.expectQuery()); // emit error.
        } else {
            throw new IllegalStateException(String.format("currentSubscriber isn' expected type[%s]."
                    , currentSubscriber.getClass().getName()));
        }
    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #setCurrentSubscriberRowMeta(ResultRowMeta)
     * @see #emitResultRowTerminator(ResultStates, boolean)
     */
    protected final boolean decodeMultiRowData(ByteBuf cumulateBuffer) {
        final ResultSubscriber currentSubscriber = Objects.requireNonNull(this.currentSubscriber, "currentSubscriber");
        if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessErrorBufferSubscriber) currentSubscriber);
        }
        final RowSink sink;
        if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER
                || currentSubscriber instanceof MonoDownstreamSubscriber) {
            // setCurrentSubscriberRowMeta have emitted error.
            sink = EMPTY_ROW_SINK;
        } else if (currentSubscriber instanceof FluxDownstreamSubscriber) {
            sink = ((FluxDownstreamSubscriber) currentSubscriber).rowSink;
        } else if (currentSubscriber instanceof FluxBufferSubscriber) {
            sink = ((FluxBufferSubscriber) currentSubscriber).sink;
        } else if (currentSubscriber instanceof MonoBufferSubscriber) {
            throw new IllegalStateException(String.format(
                    "currentSubscriber type[%s] error,please check this.setCurrentSubscriberRowMeta(ResultRowMeta)."
                    , currentSubscriber.getClass().getName()));
        } else {
            throw createUnknownResultSubscriberTypeError(currentSubscriber);
        }
        return internalDecodeMultiRowData(cumulateBuffer, sink);
    }

    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     */
    protected boolean internalDecodeMultiRowData(ByteBuf cumulateBuffer, RowSink sink) {
        throw new UnsupportedOperationException();
    }


    /**
     * must invoke in {@link io.netty.channel.EventLoop}
     *
     * @see #setCurrentSubscriberRowMeta(ResultRowMeta)
     * @see #decodeMultiRowData(ByteBuf)
     */
    protected final void emitResultRowTerminator(ResultStates states, final boolean hasMore) {
        final ResultSubscriber currentSubscriber = Objects.requireNonNull(this.currentSubscriber, "currentSubscriber");
        if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
            throw createTaskEndWithDatabaseAccessError((AccessErrorBufferSubscriber) this.currentSubscriber);
        }
        this.currentSubscriber = null; // clear for downstream
        final boolean tooManyResultError = (++this.receiveResultCount) == this.expectedResultCount && hasMore;

        if (tooManyResultError) {
            this.currentSubscriber = TOO_MANY_RESULT_ERROR_SUBSCRIBER; // don't accept subscribe again.
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
        } else if (currentSubscriber instanceof MonoBufferSubscriber) {
            throw new IllegalStateException(String.format(
                    "currentSubscriber type[%s] error,please check this.setCurrentSubscriberRowMeta(ResultRowMeta)."
                    , currentSubscriber.getClass().getName()));
        } else {
            throw createUnknownResultSubscriberTypeError(currentSubscriber);
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
        if (buffer instanceof AccessErrorBufferSubscriber) {
            AccessErrorBufferSubscriber aes = ((AccessErrorBufferSubscriber) buffer);
            JdbdSQLException e = new JdbdSQLException(aes.message, aes.accessError);
            if (subscriber instanceof MonoDownstreamSubscriber) {
                ((MonoDownstreamSubscriber) subscriber).sink.error(e);
            } else if (subscriber instanceof FluxDownstreamSubscriber) {
                ((FluxDownstreamSubscriber) subscriber).sink.error(e);
            } else {
                throw createUnknownResultSubscriberTypeError(subscriber);
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
            throw createUnknownResultSubscriberTypeError(subscriber);
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
        if (this.getTaskPhase() == null) {
            syncSubmit(sinkError);  // submit task
        } else {
            publishHeapUpBufferIfNeed();
        }
    }

    private boolean subscribePermit(Consumer<Throwable> sink) {
        final ResultSubscriber currentSubscriber = this.currentSubscriber;
        boolean permit;
        if (getTaskPhase() == TaskPhase.END
                && (this.bufferSubscriberQueue == null || this.bufferSubscriberQueue.isEmpty())) {
            sink.accept(new NoMoreResultException(String.format("%s no more result,cannot subscribe.", MultiResults.class.getName())));
            permit = false;
        } else if (currentSubscriber instanceof AccessErrorBufferSubscriber) {
            AccessErrorBufferSubscriber ae = (AccessErrorBufferSubscriber) currentSubscriber;
            sink.accept(new JdbdSQLException(ae.message, ae.accessError));
            permit = false;
        } else if (currentSubscriber == TOO_MANY_RESULT_ERROR_SUBSCRIBER) {
            sink.accept(new TooManyResultException(this.expectedResultCount, this.receiveResultCount));
            permit = false;
        } else {
            permit = true;
        }
        return permit;
    }


    /*################################## blow private static method ##################################*/

    private static <T> Queue<T> createQueue() {
        return new LinkedList<>();
    }

    private static IllegalStateException createTaskEndWithDatabaseAccessError(AccessErrorBufferSubscriber error) {
        String m = String.format("%s have ended with database access error :%s."
                , CommunicationTask.class.getName()
                , error.accessError.getMessage());
        return new IllegalStateException(m);
    }


    private static IllegalStateException createUnknownResultSubscriberTypeError(ResultSubscriber subscriber) {
        return new IllegalStateException(String.format("Unknown %s type[%s]"
                , ResultSubscriber.class.getName(), subscriber.getClass().getName()));
    }




    /*################################## blow protected  static class ##################################*/

    protected interface RowSink {

        boolean isCanceled();

        void next(ResultRow resultRow);
    }


    /*################################## blow private static inner class  ##################################*/

    private interface ResultSubscriber {

    }

    private interface BufferSubscriber extends ResultSubscriber {


    }

    private interface DownstreamSubscriber extends ResultSubscriber {

    }

    private static class AccessErrorBufferSubscriber implements BufferSubscriber {

        final SQLException accessError;

        final String message;

        AccessErrorBufferSubscriber(SQLException accessError, String message) {
            this.accessError = accessError;
            this.message = message;
        }

        AccessErrorBufferSubscriber(SQLException accessError) {
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


    private static final class FluxBufferSubscriber implements BufferSubscriber {

        private final ResultRowMeta rowMeta;

        private final Queue<ResultRow> resultRowQueue = createQueue();

        //non-volatile ,modify in netty EventLoop.
        private ResultStates resultStates;

        //non-volatile ,modify in netty EventLoop.
        private Throwable accessError;

        private boolean tooManyResultError;

        private final RowSink sink = new RowSink() {
            @Override
            public boolean isCanceled() {
                return accessError != null;
            }

            @Override
            public void next(ResultRow resultRow) {
                if (accessError == null) {
                    resultRowQueue.add(resultRow);
                }

            }
        };

        private FluxBufferSubscriber(ResultRowMeta rowMeta) {
            this.rowMeta = rowMeta;
        }
    }

    private static final class DownstreamRowSink implements RowSink {

        private final FluxDownstreamSubscriber subscriber;

        //non-volatile ,modify in netty EventLoop.
        private boolean rowDecoderError;

        private DownstreamRowSink(FluxDownstreamSubscriber subscriber) {
            if (subscriber.rowMeta == null) {
                throw new IllegalArgumentException("FluxDownstreamSubscriber.rowMeta is null");
            }
            this.subscriber = subscriber;
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
