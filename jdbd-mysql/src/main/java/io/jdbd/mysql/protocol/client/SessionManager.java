package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

interface SessionManager {

    TaskAdjutant adjutant();

    Mono<SessionManager> reset();

    Mono<Void> reConnect();

}
