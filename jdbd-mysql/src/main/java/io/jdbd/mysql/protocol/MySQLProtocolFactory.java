package io.jdbd.mysql.protocol;

import reactor.core.publisher.Mono;

public interface MySQLProtocolFactory {

    Mono<MySQLProtocol> createProtocol();


    Mono<Void> close();


}
