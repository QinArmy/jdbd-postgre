package io.jdbd.vendor;

import io.jdbd.BatchUpdateResults;
import io.jdbd.NoMoreResultException;
import io.jdbd.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;


public abstract class AbstractBatchUpdateTask extends AbstractCommunicationTask implements BatchUpdateResults {

    private FluxSink<ResultStates> sink;


    protected AbstractBatchUpdateTask(TaskAdjutant executorAdjutant) {
        super(executorAdjutant);
    }


    @Override
    public final Flux<ResultStates> batchUpdate() {
        return Flux.create(sink -> {
            if (this.executorAdjutant.inEventLoop()) {
                subscribeBatchUpdate(sink);
            } else {
                this.executorAdjutant.execute(() -> subscribeBatchUpdate(sink));
            }
        });
    }

    /*################################## blow protected method ##################################*/


    protected final FluxSink<ResultStates> obtainDownstreamSink() {
        FluxSink<ResultStates> sink = this.sink;
        if (sink == null) {
            throw new IllegalStateException("no subscriber");
        }
        return sink;
    }


    /*################################## blow private method ##################################*/

    private void subscribeBatchUpdate(FluxSink<ResultStates> sink) {
        final FluxSink<ResultStates> currentSink = this.sink;
        if (getTaskPhase() == TaskPhase.END) {
            sink.error(new NoMoreResultException(String.format("%s no more result,cannot subscribe."
                    , BatchUpdateResults.class.getName())));
        } else if (currentSink == null) {
            this.sink = sink;
            submit(sink::error);
        } else {
            sink.error(new NoMoreResultException(String.format("%s have subscribed,cannot subscribe."
                    , BatchUpdateResults.class.getName())));
        }

    }


    /*################################## blow private static inner class ##################################*/


}
