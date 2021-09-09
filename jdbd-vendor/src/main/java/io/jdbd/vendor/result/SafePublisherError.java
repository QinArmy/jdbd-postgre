package io.jdbd.vendor.result;

import io.jdbd.result.Result;
import io.jdbd.result.SafePublisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Operators;

final class SafePublisherError implements SafePublisher {

    private final Throwable error;

    SafePublisherError(Throwable error) {
        this.error = error;
    }

    @Override
    public void subscribe(Subscriber<? super Result> s) {
        Operators.error(s, this.error);
    }

}
