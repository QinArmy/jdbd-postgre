package io.jdbd.vendor.result;

import io.jdbd.result.ResultState;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@Deprecated
final class UpdateResultSubscriber_0 {

    static Mono<ResultState> create(ITaskAdjutant adjutant, Consumer<MultiResultSink> callback) {
        return Mono.empty();
    }


}