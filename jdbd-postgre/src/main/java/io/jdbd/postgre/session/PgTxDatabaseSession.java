package io.jdbd.postgre.session;

import io.jdbd.TransactionOption;
import io.jdbd.TxDatabaseSession;
import io.jdbd.pool.PoolTxDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import reactor.core.publisher.Mono;

class PgTxDatabaseSession extends PgDatabaseSession implements TxDatabaseSession {


    static PgTxDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgTxDatabaseSession(adjutant, protocol);
    }

    static PgPoolTxDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgPoolTxDatabaseSession(adjutant, protocol);
    }


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


    private static final class PgPoolTxDatabaseSession extends PgTxDatabaseSession implements PoolTxDatabaseSession {

        private PgPoolTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public final Mono<PoolTxDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public final Mono<PoolTxDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }

    }


}
