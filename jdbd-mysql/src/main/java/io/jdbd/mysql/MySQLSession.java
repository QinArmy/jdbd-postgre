package io.jdbd.mysql;

import io.jdbd.TransactionOption;
import io.jdbd.TxDatabaseSession;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.stmt.BindStatement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

class MySQLSession extends AbstractStatelessSession implements TxDatabaseSession {

    public static MySQLSession getInstance(ClientProtocol clientProtocol) {
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

    @Override
    public Publisher<TransactionOption> getTransactionOption() {
        return null;
    }

    @Override
    public BindStatement bindable(String sql) {
        return null;
    }
}
