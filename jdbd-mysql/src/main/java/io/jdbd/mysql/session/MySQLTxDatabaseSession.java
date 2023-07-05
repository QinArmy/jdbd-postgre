package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.pool.PoolTxDatabaseSession;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.LocalDatabaseSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link LocalDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLTxDatabaseSession extends MySQLDatabaseSession implements LocalDatabaseSession {


    static LocalDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLTxDatabaseSession(adjutant, protocol);
    }

    static PoolTxDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLPoolTxDatabaseSession(adjutant, protocol);
    }

    private MySQLTxDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }


    @Override
    public final Mono<LocalDatabaseSession> startTransaction(TransactionOption option) {
        return this.protocol.startTransaction(option)
                .thenReturn(this);
    }


    @Override
    public final Mono<LocalDatabaseSession> commit() {
        return this.protocol.commit()
                .thenReturn(this);
    }

    @Override
    public final Mono<LocalDatabaseSession> rollback() {
        return this.protocol.rollback()
                .thenReturn(this);
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
