package io.jdbd.vendor.result;

import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultItem;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Operators;

final class OrderedFluxError implements OrderedFlux {

    private final Throwable error;

    OrderedFluxError(Throwable error) {
        this.error = error;
    }

    @Override
    public void subscribe(Subscriber<? super ResultItem> s) {
        Operators.error(s, this.error);
    }

}
