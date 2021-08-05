package io.jdbd.vendor.task;

import reactor.core.publisher.Mono;

public interface TaskSignal {


    Mono<Void> sendPacket(CommunicationTask<?> task, boolean endTask);


}
