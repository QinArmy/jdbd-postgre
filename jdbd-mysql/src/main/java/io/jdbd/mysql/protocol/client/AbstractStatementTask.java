package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.BiFunction;
import java.util.function.Consumer;

abstract class AbstractStatementTask implements ByteBufStatementTask, MultiResults {

    final StatementTaskAdjutant taskAdjutant;

    final int negotiatedCapability;

    // non-volatile ,because all modify in netty EventLoop .
    private TaskPhase taskPhase;

    private Object sink;

    private Consumer<ResultStates> statesConsumer;

    private BiFunction<ResultRow, ResultRowMeta, ?> decoder;

    private Throwable terminateError;

    int sequenceId = -1;

    AbstractStatementTask(StatementTaskAdjutant taskAdjutant) {
        this.taskAdjutant = taskAdjutant;
        this.negotiatedCapability = taskAdjutant.obtainNegotiatedCapability();
    }

    @Nullable
    @Override
    public final ByteBuf start() {
        if (!this.taskAdjutant.inEventLoop()) {
            throw new IllegalStateException("start() isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw new IllegalStateException("taskPhase not null");
        }
        ByteBuf byteBuf = internalStart();
        this.taskPhase = TaskPhase.STARTED;
        return byteBuf;
    }

    @Override
    public final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }

    @Override
    public ByteBuf moreSendPacket() {
        return null;
    }


    final void emitTerminatedError(Object sink, Throwable e) {

        if (sink instanceof MonoSink) {
            ((MonoSink<?>) sink).error(e);
        } else if (sink instanceof FluxSink) {
            ((FluxSink<?>) sink).error(e);
        } else {
            throw new IllegalArgumentException(String.format("Unknown sink type[%s]", sink.getClass().getName()));
        }
    }

    final boolean ignoreEmit() {
        return this.terminateError != null;
    }

    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId % 256;
    }

    final void setTerminateError(Throwable e) {
        this.terminateError = e;
    }

    final FluxSink<Object> obtainFluxSink() {
        throw new IllegalStateException();
    }

    final BiFunction<ResultRow, ResultRowMeta, ?> obtainRowDecoder() {
        throw new IllegalStateException();
    }

    final void emitErrorPacket(ErrorPacket error) {
        emitTerminatedError(this.sink, MySQLExceptionUtils.createErrorPacketException(error));
        this.taskPhase = TaskPhase.END;
        this.sink = null;
        this.decoder = null;
        this.statesConsumer = null;
    }

    final void emitUpdateOkPacket(OkPacket ok) {

    }


    @Nullable
    abstract ByteBuf internalStart();

    void onSubscribeInEventLoop() {

    }

    void onErrorInEventLoop(Throwable e) {

    }

    void onCompleteInEventLoop() {

    }

    private void onSubscribe(Object sink, @Nullable BiFunction<ResultRow, ResultRowMeta, ?> decoder
            , Consumer<ResultStates> statesConsumer) {
        if (this.taskAdjutant.inEventLoop()) {
            doOnSubscribe(sink, decoder, statesConsumer);
        } else {
            this.taskAdjutant.executeInEventLoop(() -> doOnSubscribe(sink, decoder, statesConsumer));
        }
    }

    private void doOnSubscribe(Object sink, @Nullable BiFunction<ResultRow, ResultRowMeta, ?> decoder
            , Consumer<ResultStates> statesConsumer) {
        if (this.taskPhase == null) {
            this.taskAdjutant.submitTask(this);
            this.taskPhase = TaskPhase.SUBMITTED;
        } else if (this.taskPhase == TaskPhase.END) {
            JdbdMySQLException e;
            e = new JdbdMySQLException("%s terminated,can't accept subscribe.", MultiResults.class.getName());
            emitTerminatedError(sink, e);
            return;
        }
        this.sink = sink;
        this.decoder = decoder;
        this.statesConsumer = statesConsumer;
        this.onSubscribeInEventLoop();
    }


    private void onError(Throwable e) {
        if (this.taskAdjutant.inEventLoop()) {
            doOnError(e);
        } else {
            this.taskAdjutant.executeInEventLoop(() -> doOnError(e));
        }
    }

    private void doOnError(Throwable e) {
        if (this.taskPhase == TaskPhase.END) {
            return;
        }
        try {
            onErrorInEventLoop(e);
        } finally {
            this.taskPhase = TaskPhase.END;
        }
    }


    private void onComplete() {
        if (this.taskAdjutant.inEventLoop()) {
            doOnComplete();
        } else {
            this.taskAdjutant.executeInEventLoop(this::doOnComplete);
        }
    }

    private void doOnComplete() {
        if (this.taskPhase == TaskPhase.END) {
            return;
        }
        try {
            this.onCompleteInEventLoop();
        } finally {
            this.taskPhase = TaskPhase.END;
        }
    }


    @Override
    public final Mono<Long> nextUpdate(Consumer<ResultStates> statesConsumer) {
        return new TerminateMono<>(Mono.create(sink -> onSubscribe(sink, null, statesConsumer)))
                ;
    }

    @Override
    public final <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> decoder
            , Consumer<ResultStates> statesConsumer) {
        return new TerminateFlux<>(Flux.create(sink -> onSubscribe(sink, decoder, statesConsumer)));
    }

    private final class TerminateMono<T> extends Mono<T> {

        private final Mono<T> source;

        private TerminateMono(Mono<T> source) {
            this.source = source;
        }


        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            this.source.subscribe(new TerminateSubscribe<>(actual));
        }
    }

    private final class TerminateFlux<T> extends Flux<T> {

        private final Flux<T> source;

        private TerminateFlux(Flux<T> source) {
            this.source = source;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            this.source.subscribe(new TerminateSubscribe<>(actual));
        }
    }

    private final class TerminateSubscribe<T> implements CoreSubscriber<T> {

        private final CoreSubscriber<? super T> actual;

        private TerminateSubscribe(CoreSubscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.actual.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            this.actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            try {
                this.actual.onError(t);
            } finally {
                AbstractStatementTask.this.onError(t);
            }

        }

        @Override
        public void onComplete() {
            try {
                this.actual.onComplete();
            } finally {
                AbstractStatementTask.this.onComplete();
            }
        }
    }


}
