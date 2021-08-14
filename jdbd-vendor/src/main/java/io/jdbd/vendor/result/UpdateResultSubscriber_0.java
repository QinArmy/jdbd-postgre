package io.jdbd.vendor.result;

import io.jdbd.result.NoMoreResultException;
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

import java.util.List;
import java.util.function.Consumer;

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

    private ResultState resultState;


    private UpdateResultSubscriber_0(ITaskAdjutant adjutant, MonoSink<ResultState> sink) {
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
            addError(ResultType.QUERY);
            Flux.from(singleResult.receiveQuery())
                    .doOnError(this::printUpstreamErrorAfterSkip)
                    .subscribe();
        } else if (singleResult.getIndex() == 0) {
            Mono.from(singleResult.receiveUpdate())
                    .doOnSuccess(this::setResultState)
                    .doOnError(this::addUpstreamError)
                    .subscribe();
        } else {
            addError(ResultType.MULTI_RESULT);
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
        return ResultType.UPDATE;
    }

    private void printUpstreamErrorAfterSkip(Throwable error) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Skip error type occur error.", error);
        }
    }

    private void doCompleteInEventLoop() {
        final List<Throwable> errorList = this.errorList;

        if (errorList == null || errorList.isEmpty()) {
            final ResultState resultState = this.resultState;
            if (resultState == null) {
                this.sink.error(new NoMoreResultException("No receive any result from upstream."));
            } else {
                this.sink.success(resultState);
            }
        } else {
            this.sink.error(JdbdExceptions.createException(errorList));
        }

    }

    private void setResultState(ResultState resultState) {
        if (this.adjutant.inEventLoop()) {
            this.resultState = resultState;
        } else {
            this.adjutant.execute(() -> this.resultState = resultState);
        }
    }


}
