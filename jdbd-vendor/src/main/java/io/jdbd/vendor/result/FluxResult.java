package io.jdbd.vendor.result;

import io.jdbd.result.Result;
import io.jdbd.result.SafePublisher;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

/**
 * @see UpdateResultSubscriber
 * @see QueryResultSubscriber
 * @see BatchUpdateResultSubscriber
 * @see MultiResultSubscriber
 */
final class FluxResult implements SafePublisher {

    static FluxResult create(Consumer<FluxResultSink> callBack) {
        return new FluxResult(callBack);
    }

    private final Consumer<FluxResultSink> callBack;

    private FluxResult(Consumer<FluxResultSink> callBack) {
        this.callBack = callBack;
    }

    @Override
    public final void subscribe(Subscriber<? super Result> actual) {
        FluxResultSinkImpl sink = new FluxResultSinkImpl(actual);
        actual.onSubscribe(sink.subscription);

        try {
            this.callBack.accept(sink);
        } catch (Throwable e) {
            Exceptions.throwIfJvmFatal(e);
            actual.onError(JdbdExceptions.wrap(e));
        }

    }

    private static final class FluxResultSinkImpl implements FluxResultSink {

        private final Subscriber<? super Result> subscriber;

        private final SubscriptionImpl subscription;

        private FluxResultSinkImpl(Subscriber<? super Result> subscriber) {
            this.subscriber = subscriber;
            this.subscription = new SubscriptionImpl();
        }

        @Override
        public final void error(Throwable e) {
            // this method invoker in EventLoop
            this.subscriber.onError(JdbdExceptions.wrap(e));
        }

        @Override
        public final void complete() {
            // this method invoker in EventLoop
            this.subscriber.onComplete();
        }

        @Override
        public final boolean isCancelled() {
            // this method invoker in EventLoop
            return this.subscription.canceled == 1;
        }

        @Override
        public final void next(Result result) {
            // this method invoker in EventLoop
            this.subscriber.onNext(result);
        }

        @Override
        public final ResultSink froResultSet() {
            return new ResultSinkImpl(this.subscriber, this.subscription);
        }
    }

    private static final class ResultSinkImpl implements ResultSink {

        private final Subscriber<? super Result> subscriber;

        private final SubscriptionImpl subscription;

        private ResultSinkImpl(Subscriber<? super Result> subscriber, SubscriptionImpl subscription) {
            this.subscriber = subscriber;
            this.subscription = subscription;
        }

        @Override
        public final boolean isCancelled() {
            // this method invoker in EventLoop
            boolean cancelled = this.subscription.canceled == 1;
            if (!cancelled && this.subscriber instanceof ResultSubscriber) {
                cancelled = ((ResultSubscriber) this.subscriber).isCancelled();
            }
            return cancelled;
        }

        @Override
        public final void next(Result result) {
            // this method invoker in EventLoop
            this.subscriber.onNext(result);
        }

    }

    private static final class SubscriptionImpl implements Subscription {

        private static final AtomicIntegerFieldUpdater<SubscriptionImpl> CANCELED =
                AtomicIntegerFieldUpdater.newUpdater(SubscriptionImpl.class, "canceled");


        private SubscriptionImpl() {
        }

        private volatile int canceled;

        @Override
        public final void request(long n) {
            //no-op ,because subscriber :
            // 1. io.jdbd.vendor.result.UpdateResultSubscriber
            // 2. io.jdbd.vendor.result.QueryResultSubscriber
            // 3. io.jdbd.vendor.result.BatchUpdateResultSubscriber
            // 4. io.jdbd.vendor.result.MultiResultSubscriber
            // always Long.MAX_VALUE
        }

        @Override
        public final void cancel() {
            // this method invoker in EventLoop
            CANCELED.set(this, 1);
        }

    }


}
