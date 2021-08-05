package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Server;
import reactor.core.publisher.Mono;

interface ConnectionManager {

    Mono<Void> connect();

    Mono<Void> reConnect();

    Mono<Server> reset();


}
