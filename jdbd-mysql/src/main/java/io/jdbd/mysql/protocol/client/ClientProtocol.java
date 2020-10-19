package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import reactor.core.publisher.Mono;

public interface ClientProtocol {

    int CLIENT_PLUGIN_AUTH = 1 << 19;

    Mono<MySQLPacket> handshake();

    Mono<MySQLPacket> responseHandshake();

    Mono<MySQLPacket> sslRequest();

}
