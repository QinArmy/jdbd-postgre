package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.pool.PoolTxDatabaseSession;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TxDatabaseSession;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link TxDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLTxDatabaseSession extends MySQLDatabaseSession implements TxDatabaseSession {


    static TxDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLTxDatabaseSession(adjutant, protocol);
    }

    static PoolTxDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLPoolTxDatabaseSession(adjutant, protocol);
    }

    private MySQLTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }

    @Override
    public final Mono<TransactionOption> getTransactionOption() {
        return null;
    }

    @Override
    public final Mono<TxDatabaseSession> startTransaction(TransactionOption option) {
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


    /**
     * <p>
     * This class is implementation of {@link PoolTxDatabaseSession} with MySQL client protocol.
     * </p>
     */
    private static final class MySQLPoolTxDatabaseSession extends MySQLTxDatabaseSession
            implements PoolTxDatabaseSession {

        private MySQLPoolTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolTxDatabaseSession> ping(int timeoutSeconds) {
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
