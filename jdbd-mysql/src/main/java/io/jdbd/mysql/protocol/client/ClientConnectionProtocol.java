package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import reactor.core.publisher.Mono;

public interface ClientConnectionProtocol extends ClientProtocol {

    Mono<MySQLPacket> ssl();

    Mono<MySQLPacket> receiveHandshake();

    Mono<Void> responseHandshakeAndAuthenticate();

}
