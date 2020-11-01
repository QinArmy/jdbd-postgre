package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.netty.Connection;

final class ReceiveOneMono extends Mono<ByteBuf> {

    private final Flux<ByteBuf> source;

    private ReceiveOneMono(Flux<ByteBuf> source) {
        this.source = source;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
        this.source.subscribe(new ReceiveOneSubscriber(actual));
    }

    static Mono<ByteBuf> receiveOneMono(Connection connection) {
        return connection.inbound().receive().elementAt(0);
    }


    private static final class ReceiveOneSubscriber implements CoreSubscriber<ByteBuf>, Subscription {

        private final CoreSubscriber<? super ByteBuf> actual;

        private Subscription s;

        private ReceiveOneSubscriber(CoreSubscriber<? super ByteBuf> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            this.actual.onSubscribe(this);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            this.actual.onNext(byteBuf);
            //  this.cancel();
        }

        @Override
        public void onError(Throwable t) {
            this.actual.onError(t);
        }

        @Override
        public void onComplete() {
            this.actual.onComplete();
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                this.s.request(1L);
            }
        }

        @Override
        public void cancel() {
            this.s.cancel();
        }
    }


}
