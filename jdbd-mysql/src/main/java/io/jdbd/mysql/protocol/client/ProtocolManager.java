package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

interface ProtocolManager {

    TaskAdjutant adjutant();

    Mono<ProtocolManager> reset();

    Mono<Void> reConnect();

}
