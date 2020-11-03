package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLAssert;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class MySQLPacketSubscriber<T> implements CoreSubscriber<T>, MySQLPacketReceiver<T> {

    private final AtomicReference<Subscription> subscriptionHolder = new AtomicReference<>(null);

    private Subscription subscription;

    private final AtomicReference<MySQLPacketProxySubscriber<T>> actualSubscriber = new AtomicReference<>(null);


    public void subscribe(MySQLPacketProxySubscriber<T> subscriber) {
        if (this.actualSubscriber.compareAndSet(null, subscriber)) {
            subscriber.onSubscribe();
        } else {
            throw new IllegalStateException("exists subscriber.");
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (this.subscriptionHolder.compareAndSet(null, s)) {
            this.subscription = s;
        }
    }

    @Override
    public void onNext(T t) {
        MySQLPacketProxySubscriber<T> subscriber = this.actualSubscriber.get();
        MySQLAssert.stateNonNull(subscriber, "actualSubscriber is null.");
        subscriber.onNext(t);

        subscriber.onComplete();
        this.actualSubscriber.compareAndSet(subscriber, null);
    }

    @Override
    public void onError(Throwable t) {
        MySQLPacketProxySubscriber<T> subscriber = this.actualSubscriber.get();
        MySQLAssert.stateNonNull(subscriber, "actualSubscriber is null.");
        subscriber.onError(t);

        this.actualSubscriber.compareAndSet(subscriber, null);
    }

    @Override
    public void onComplete() {
        // no-op
    }


    @Override
    public Mono<T> receiveOne() {
        Subscription subscription = this.subscription;
        if (subscription == null) {
            subscription = this.subscriptionHolder.get();
        }
        return new MySQLReceiveMono<>(this, subscription);
    }


    private static final class MySQLReceiveMono<T> extends Mono<T> implements Subscription {

        private final MySQLPacketSubscriber<T> source;

        private final Subscription actualSubscription;

        private final AtomicBoolean canceled = new AtomicBoolean(false);

        private final AtomicInteger requested = new AtomicInteger(0);

        private MySQLReceiveMono(MySQLPacketSubscriber<T> source, Subscription actualSubscription) {
            this.source = source;
            this.actualSubscription = actualSubscription;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            this.source.subscribe(new MySQLPacketProxySubscriber<>(this, actual));
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n) && !this.canceled.get() && this.requested.addAndGet(1) == 1) {
                this.actualSubscription.request(1L);
            }
        }

        @Override
        public void cancel() {
            this.canceled.compareAndSet(false, true);
        }
    }


    private static final class MySQLPacketProxySubscriber<T> {

        private final Subscription subscription;

        private final CoreSubscriber<? super T> actual;

        private MySQLPacketProxySubscriber(Subscription subscription, CoreSubscriber<? super T> actual) {
            this.subscription = subscription;
            this.actual = actual;
        }

        public void onSubscribe() {
            this.actual.onSubscribe(this.subscription);
        }

        public void onNext(T t) {
            this.actual.onNext(t);
        }

        public void onError(Throwable t) {
            this.actual.onError(t);
        }

        public void onComplete() {
            this.actual.onComplete();
        }
    }

}
