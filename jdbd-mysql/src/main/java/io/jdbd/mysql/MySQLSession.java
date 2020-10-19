package io.jdbd.mysql;

import io.jdbd.DatabaseSession;
import io.jdbd.TransactionOption;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import reactor.core.publisher.Mono;

class MySQLSession extends AbstractStatelessSession implements DatabaseSession {

    public static MySQLSession getInstance(ClientProtocol clientProtocol){
        return new MySQLSession(clientProtocol);
    }

    private final ClientProtocol clientProtocol;

    private MySQLSession(ClientProtocol clientProtocol) {
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
