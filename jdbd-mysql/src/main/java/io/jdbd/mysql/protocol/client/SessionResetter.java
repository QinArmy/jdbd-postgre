package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import reactor.core.publisher.Mono;

interface SessionResetter {

    Mono<Server> reset();
}
