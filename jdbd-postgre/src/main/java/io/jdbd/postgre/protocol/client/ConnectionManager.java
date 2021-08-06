package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Server;
import io.jdbd.vendor.task.TaskExecutor;
import reactor.core.publisher.Mono;

interface ConnectionManager {

    Mono<Void> reConnect();

    Mono<Server> reset();

    /**
     * @return always same instance.
     * @see TaskExecutor#taskAdjutant()
     */
    TaskAdjutant taskAdjutant();

}
