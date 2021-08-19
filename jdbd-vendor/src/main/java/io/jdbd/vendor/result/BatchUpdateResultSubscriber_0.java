package io.jdbd.vendor.result;

import io.jdbd.result.ResultState;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Deprecated
final class BatchUpdateResultSubscriber_0 {

    static Flux<ResultState> create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return Flux.empty();
    }


}
