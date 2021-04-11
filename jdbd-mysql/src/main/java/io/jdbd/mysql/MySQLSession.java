package io.jdbd.mysql;

import io.jdbd.BindableStatement;
import io.jdbd.StaticStatement;
import io.jdbd.TransactionOption;
import io.jdbd.TxDatabaseSession;
import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import reactor.core.publisher.Mono;

class MySQLSession extends AbstractStatelessSession implements TxDatabaseSession {

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


    @Override
    public StaticStatement staticStmt() {
        return null;
    }

    @Override
    public BindableStatement bindableStmt(String sql) {
        return null;
    }
}
