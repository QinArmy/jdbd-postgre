package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.task.TaskExecutor;
import reactor.core.publisher.Mono;

import java.util.Map;

interface ConnectionManager {

    Mono<Void> reConnect();

    Mono<Void> reset(Map<String, String> initializedParamMap);

    /**
     * @return always same instance.
     * @see TaskExecutor#taskAdjutant()
     */
    TaskAdjutant taskAdjutant();

}
