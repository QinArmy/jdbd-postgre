package io.jdbd.vendor.result;

import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultItem;
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
final class FluxResult implements OrderedFlux {

    static FluxResult create(Consumer<ResultSink> callBack) {
        return new FluxResult(callBack);
    }

    private final Consumer<ResultSink> callBack;

    private FluxResult(Consumer<ResultSink> callBack) {
        this.callBack = callBack;
    }

    @Override
    public void subscribe(Subscriber<? super ResultItem> actual) {
        ResultSinkImpl sink = new ResultSinkImpl(actual);
        actual.onSubscribe(sink.subscription);

        try {
            this.callBack.accept(sink);
        } catch (Throwable e) {
            Exceptions.throwIfJvmFatal(e);
            actual.onError(JdbdExceptions.wrap(e));
        }

    }

    private static final class ResultSinkImpl implements ResultSink {

        private final Subscriber<? super ResultItem> subscriber;

        private final SubscriptionImpl subscription;

        private ResultSinkImpl(Subscriber<? super ResultItem> subscriber) {
            this.subscriber = subscriber;
            this.subscription = new SubscriptionImpl();
        }


        @Override
        public void error(Throwable e) {
            // this method invoker in EventLoop
            this.subscriber.onError(JdbdExceptions.wrap(e));
        }

        @Override
        public void complete() {
            // this method invoker in EventLoop
            this.subscriber.onComplete();
        }

        @Override
        public boolean isCancelled() {
            // this method invoker in EventLoop
            return this.subscription.canceled == 1;
        }

        @Override
        public void next(ResultItem result) {
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
        public void request(long n) {
            //no-op ,because subscriber :
            // 1. io.jdbd.vendor.result.UpdateResultSubscriber
            // 2. io.jdbd.vendor.result.QueryResultSubscriber
            // 3. io.jdbd.vendor.result.BatchUpdateResultSubscriber
            // 4. io.jdbd.vendor.result.MultiResultSubscriber
            // always Long.MAX_VALUE
        }

        @Override
        public void cancel() {
            // this method invoker in EventLoop
            CANCELED.set(this, 1);
        }

    }


}
