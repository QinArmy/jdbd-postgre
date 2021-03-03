package io.jdbd.mysql;

import io.jdbd.JdbdSession;
import io.jdbd.TransactionOption;
import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import reactor.core.publisher.Mono;

class MySQLSession extends AbstractStatelessSession implements JdbdSession {

    public static MySQLSession getInstance(ClientCommandProtocol clientProtocol) {
        return new MySQLSession(clientProtocol);
    }

    private final ClientCommandProtocol clientProtocol;

    private MySQLSession(ClientCommandProtocol clientProtocol) {
        this.clientProtocol = clientProtocol;
    }

    @Override
    public Mono<Void> startTransaction(TransactionOption option) {
        return null;
    }

    @Override
    public Mono<Void> commit() {
        return null;
    }

    @Override
    public Mono<Void> rollback() {
        return null;
    }

}
