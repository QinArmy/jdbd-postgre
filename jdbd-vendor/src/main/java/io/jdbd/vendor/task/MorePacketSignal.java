package io.jdbd.vendor.task;

import reactor.core.publisher.Mono;

public interface MorePacketSignal {


    Mono<Void> sendPacket(CommunicationTask task);


}
