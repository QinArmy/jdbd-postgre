package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.AbstractSQLCommTask;

abstract class MySQLCommandTask extends AbstractSQLCommTask implements MySQLTask {

    final MySQLTaskAdjutant executorAdjutant;

    final int negotiatedCapability;

    private int sequenceId = -1;

    MySQLCommandTask(MySQLTaskAdjutant executorAdjutant, int expectedResultCount) {
        super(executorAdjutant, expectedResultCount);
        this.negotiatedCapability = executorAdjutant.obtainNegotiatedCapability();
        this.executorAdjutant = executorAdjutant;
    }

    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId % 256;
    }

    @Override
    public final int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) % 256;
        this.sequenceId = sequenceId;
        return sequenceId;
    }


//
//    final void emitUpdateOkPacket(OkPacket ok) {
//
//    }
//
//
//    void onSubscribeInEventLoop() {
//
//    }
//
//    void onErrorInEventLoop(Throwable e) {
//
//    }
//
//    void onCompleteInEventLoop() {
//
//    }
//
//    private void onSubscribe(Object sink, @Nullable BiFunction<ResultRow, ResultRowMeta, ?> decoder
//            , Consumer<ResultStates> statesConsumer) {
//        if (this.executorAdjutant.inEventLoop()) {
//            doOnSubscribe(sink, decoder, statesConsumer);
//        } else {
//            this.executorAdjutant.executeInEventLoop(() -> doOnSubscribe(sink, decoder, statesConsumer));
//        }
//    }
//
//    private void doOnSubscribe(Object sink, @Nullable BiFunction<ResultRow, ResultRowMeta, ?> decoder
//            , Consumer<ResultStates> statesConsumer) {
//        if (this.taskPhase == null) {
//            this.executorAdjutant.submitTask(this);
//            this.taskPhase = TaskPhase.SUBMITTED;
//        } else if (this.taskPhase == TaskPhase.END) {
//            JdbdMySQLException e;
//            e = new JdbdMySQLException("%s terminated,can't accept subscribe.", MultiResults.class.getName());
//            emitTerminatedError(sink, e);
//            return;
//        }
//        this.sink = sink;
//        this.decoder = decoder;
//        this.statesConsumer = statesConsumer;
//        this.onSubscribeInEventLoop();
//    }
//
//
//    private void onError(Throwable e) {
//        if (this.executorAdjutant.inEventLoop()) {
//            doOnError(e);
//        } else {
//            this.executorAdjutant.executeInEventLoop(() -> doOnError(e));
//        }
//    }
//
//    private void doOnError(Throwable e) {
//        if (this.taskPhase == TaskPhase.END) {
//            return;
//        }
//        try {
//            onErrorInEventLoop(e);
//        } finally {
//            this.taskPhase = TaskPhase.END;
//        }
//    }
//
//
//    private void onComplete() {
//        if (this.executorAdjutant.inEventLoop()) {
//            doOnComplete();
//        } else {
//            this.executorAdjutant.executeInEventLoop(this::doOnComplete);
//        }
//    }
//
//    private void doOnComplete() {
//        if (this.taskPhase == TaskPhase.END) {
//            return;
//        }
//        try {
//            this.onCompleteInEventLoop();
//        } finally {
//            this.taskPhase = TaskPhase.END;
//        }
//    }
//
//
//
//    private final class TerminateMono<T> extends Mono<T> {
//
//        private final Mono<T> source;
//
//        private TerminateMono(Mono<T> source) {
//            this.source = source;
//        }
//
//
//        @Override
//        public void subscribe(CoreSubscriber<? super T> actual) {
//            this.source.subscribe(new TerminateSubscribe<>(actual));
//        }
//    }
//
//    private final class TerminateFlux<T> extends Flux<T> {
//
//        private final Flux<T> source;
//
//        private TerminateFlux(Flux<T> source) {
//            this.source = source;
//        }
//
//        @Override
//        public void subscribe(CoreSubscriber<? super T> actual) {
//            this.source.subscribe(new TerminateSubscribe<>(actual));
//        }
//    }
//
//    private final class TerminateSubscribe<T> implements CoreSubscriber<T> {
//
//        private final CoreSubscriber<? super T> actual;
//
//        private TerminateSubscribe(CoreSubscriber<? super T> actual) {
//            this.actual = actual;
//        }
//
//        @Override
//        public void onSubscribe(Subscription s) {
//            this.actual.onSubscribe(s);
//        }
//
//        @Override
//        public void onNext(T t) {
//            this.actual.onNext(t);
//        }
//
//        @Override
//        public void onError(Throwable t) {
//            try {
//                this.actual.onError(t);
//            } finally {
//                MySQLConnectionTask.this.onError(t);
//            }
//
//        }
//
//        @Override
//        public void onComplete() {
//            try {
//                this.actual.onComplete();
//            } finally {
//                MySQLConnectionTask.this.onComplete();
//            }
//        }
//    }


}
