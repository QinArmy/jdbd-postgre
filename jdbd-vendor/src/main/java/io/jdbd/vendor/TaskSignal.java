package io.jdbd.vendor;

import reactor.core.publisher.Mono;

public interface TaskSignal<T> {

    Mono<Void> terminate(CommunicationTask<T> task);

    Mono<Void> sendPacket(CommunicationTask<T> task);

    boolean canSignal();

}
