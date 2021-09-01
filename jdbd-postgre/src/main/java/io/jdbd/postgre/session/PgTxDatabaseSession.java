package io.jdbd.postgre.session;

import io.jdbd.TransactionOption;
import io.jdbd.TxDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import reactor.core.publisher.Mono;

final class PgTxDatabaseSession extends PgDatabaseSession implements TxDatabaseSession {


    private PgTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }

    @Override
    public final Mono<TransactionOption> getTransactionOption() {
        return null;
    }

    @Override
    public final Mono<Void> startTransaction(TransactionOption option) {
        return null;
    }

    @Override
    public final Mono<Void> commit() {
        return null;
    }

    @Override
    public final Mono<Void> rollback() {
        return null;
    }


}
