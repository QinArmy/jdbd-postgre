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
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

@Deprecated
final class UpdateResultSubscriber_0 extends AbstractResultSubscriber<SingleResult> {

    static Mono<ResultState> create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        final Flux<SingleResult> flux = Flux.create(sink -> {
            try {
                callback.accept(MultiResultFluxSink.create(sink, adjutant));
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
        return Mono.create(sink -> flux.subscribe(new UpdateResultSubscriber_0(adjutant, sink)));
    }

    private static final Logger LOG = LoggerFactory.getLogger(UpdateResultSubscriber_0.class);

    private final MonoSink<ResultState> sink;


    private UpdateResultSubscriber_0(ITaskAdjutant adjutant, MonoSink<ResultState> sink) {
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
        } else if (singleResult.getIndex() == 0) {
            Mono.from(singleResult.receiveUpdate())
                    .doOnSuccess(this::setResultState)
                    .subscribe();
        } else {
            addSubscribeError(ResultType.MULTI_RESULT);
        }
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
        return ResultType.UPDATE;
    }

    private void printUpstreamErrorAfterSkip(Throwable error) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Skip error type occur error.", error);
        }
    }


    private void setResultState(ResultState resultState) {

    }


}
