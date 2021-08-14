package io.jdbd.vendor.result;

import io.jdbd.result.Result;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

import java.util.function.Consumer;

final class FluxResult implements Publisher<Result> {

    static FluxResult create(ITaskAdjutant adjutant, Consumer<FluxResultSink> callBack) {
        return new FluxResult(adjutant, callBack);
    }


    private final ITaskAdjutant adjutant;

    private final Consumer<FluxResultSink> callBack;

    private FluxResult(ITaskAdjutant adjutant, Consumer<FluxResultSink> callBack) {
        this.adjutant = adjutant;
        this.callBack = callBack;
    }

    @Override
    public final void subscribe(Subscriber<? super Result> actual) {
        FluxResultSinkImpl sink = new FluxResultSinkImpl(actual, this.adjutant);
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

        private FluxResultSinkImpl(Subscriber<? super Result> subscriber, ITaskAdjutant adjutant) {
            this.subscriber = subscriber;
            this.subscription = new SubscriptionImpl(adjutant);
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
            return this.subscription.canceled;
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
            return this.subscription.canceled;
        }

        @Override
        public final void next(Result result) {
            // this method invoker in EventLoop
            this.subscriber.onNext(result);
        }

    }

    private static final class SubscriptionImpl implements Subscription {

        private final ITaskAdjutant adjutant;

        private SubscriptionImpl(ITaskAdjutant adjutant) {
            this.adjutant = adjutant;
        }

        private boolean canceled;

        @Override
        public final void request(long n) {
            //no-op ,because subscriber :
            // 1. io.jdbd.vendor.result.UpdateResultSubscriber
            // 2.
            // always Long.MAX_VALUE
        }

        @Override
        public final void cancel() {
            if (!this.canceled) {
                if (this.adjutant.inEventLoop()) {
                    this.canceled = true;
                } else {
                    this.adjutant.execute(() -> this.canceled = true);
                }
            }

        }

    }


}
