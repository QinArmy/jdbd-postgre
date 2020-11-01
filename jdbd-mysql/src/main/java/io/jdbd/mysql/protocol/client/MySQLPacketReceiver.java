package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

public interface MySQLPacketReceiver<T> {

    Mono<T> receiveOne();

}
