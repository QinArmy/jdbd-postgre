package io.jdbd.vendor.result;

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
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

@Deprecated
final class BatchUpdateResultSubscriber_0 extends AbstractResultSubscriber<SingleResult> {

    static Flux<ResultState> create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        final Flux<SingleResult> flux = Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Flux.create(sink -> flux.subscribe(new BatchUpdateResultSubscriber_0(adjutant, sink)));
    }

    private static final Logger LOG = LoggerFactory.getLogger(BatchUpdateResultSubscriber_0.class);

    private final FluxSink<ResultState> sink;

    private BatchUpdateResultSubscriber_0(ITaskAdjutant adjutant, FluxSink<ResultState> sink) {
        super(adjutant);
        this.sink = sink;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public final void onNext(SingleResult singleResult) {
        if (singleResult.isQuery()) {
            addSubscribeError(ResultType.QUERY);
            Flux.from(singleResult.receiveQuery())
                    .doOnError(this::printUpstreamErrorAfterSkip)
                    .subscribe();
        } else {
            Mono.from(singleResult.receiveUpdate())
                    .doOnSuccess(this.sink::next)
                    .doOnError(this.sink::error)
                    .subscribe();
        }
    }

    @Override
    public final void onError(Throwable t) {
        this.sink.error(t);
    }

    @Override
    public final void onComplete() {
        if (this.adjutant.inEventLoop()) {
            doCompleteInEventLoop();
        } else {
            this.adjutant.execute(this::doCompleteInEventLoop);
        }
    }

    @Override
    final ResultType getSubscribeType() {
        return ResultType.BATCH_UPDATE;
    }

    private void doCompleteInEventLoop() {
        final List<Throwable> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            this.sink.complete();
        } else {
            this.sink.error(JdbdExceptions.createException(errorList));
        }

    }

    private void printUpstreamErrorAfterSkip(Throwable error) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Skip error type occur error.", error);
        }
    }


}
