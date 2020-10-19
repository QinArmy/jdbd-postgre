package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

public interface ClientProtocol {

    int CLIENT_PLUGIN_AUTH = 1 << 19;

    Mono<AbstractHandshakePacket> connect();

}
