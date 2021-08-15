package io.jdbd.vendor.result;

import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.ResultType;
import io.jdbd.vendor.task.ITaskAdjutant;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.function.Consumer;

@Deprecated
final class QueryResultSubscriber_0 extends AbstractResultSubscriber<SingleResult> {

    static Flux<ResultRow> create(ITaskAdjutant adjutant, Consumer<ResultState> stateConsumer
            , Consumer<MultiResultSink> callback) {
        final Flux<SingleResult> flux = Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Flux.create(sink -> flux.subscribe(new QueryResultSubscriber_0(adjutant, sink, stateConsumer)));
    }

    private static final Logger LOG = LoggerFactory.getLogger(QueryResultSubscriber_0.class);

    private final FluxSink<ResultRow> sink;

    private final Consumer<ResultState> stateConsumer;

    private boolean receiveFirstResult;

    private QueryResultSubscriber_0(ITaskAdjutant adjutant, FluxSink<ResultRow> sink
            , Consumer<ResultState> stateConsumer) {
        this.sink = sink;
        this.stateConsumer = stateConsumer;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(SingleResult singleResult) {

    }

    @Override
    public final void onError(Throwable t) {
        this.sink.error(t);
    }

    @Override
    public final void onComplete() {

    }


    @Override
    final ResultType getSubscribeType() {
        return ResultType.QUERY;
    }

    private void printUpstreamErrorAfterSkip(Throwable error) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Skip error type occur error.", error);
        }
    }

    private void doCompleteInEventLoop() {
        final List<Throwable> errorList = this.errorList;

        if (errorList == null || errorList.isEmpty()) {
            if (this.receiveFirstResult) {
                this.sink.complete();
            } else {
                this.sink.error(new NoMoreResultException("No receive any result from upstream."));
            }
        } else {
            this.sink.error(JdbdExceptions.createException(errorList));
        }

    }


}
