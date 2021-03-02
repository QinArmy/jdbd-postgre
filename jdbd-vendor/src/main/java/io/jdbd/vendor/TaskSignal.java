package io.jdbd.vendor;

import reactor.core.publisher.Mono;

public interface TaskSignal {

    Mono<Void> terminate(CommunicationTask<?> task);

    Mono<Void> sendPacket(CommunicationTask<?> task);

    boolean canSignal();

}
