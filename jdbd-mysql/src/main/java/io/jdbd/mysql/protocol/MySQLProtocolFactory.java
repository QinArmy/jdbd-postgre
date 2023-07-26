package io.jdbd.mysql.protocol;

import io.jdbd.session.Closeable;
import reactor.core.publisher.Mono;

public interface MySQLProtocolFactory extends Closeable {

    Mono<MySQLProtocol> createProtocol();


}
