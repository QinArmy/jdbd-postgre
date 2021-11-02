package io.jdbd.postgre.session;

import io.jdbd.pool.PoolTxDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TxDatabaseSession;
import reactor.core.publisher.Mono;

class PgTxDatabaseSession extends PgDatabaseSession implements TxDatabaseSession {


    static PgTxDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgTxDatabaseSession(adjutant, protocol);
    }

    static PgTxDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgPoolTxDatabaseSession(adjutant, protocol);
    }


    private PgTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }


    @Override
    public final Mono<TxDatabaseSession> startTransaction(TransactionOption option) {
        return null;
    }

    @Override
    public final Mono<TxDatabaseSession> setTransactionOption(TransactionOption option) {
        return null;
    }

    @Override
    public final Mono<TxDatabaseSession> commit() {
        return null;
    }

    @Override
    public final Mono<TxDatabaseSession> rollback() {
        return null;
    }


    private static final class PgPoolTxDatabaseSession extends PgTxDatabaseSession implements PoolTxDatabaseSession {

        private PgPoolTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolTxDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolTxDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }

    }


}
