package io.jdbd.vendor.result;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Deprecated
final class QueryResultSubscriber_0 {

    static Flux<ResultRow> create(ITaskAdjutant adjutant, Consumer<ResultStates> stateConsumer
            , Consumer<MultiResultSink> callback) {
        return Flux.empty();
    }


}
