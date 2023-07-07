package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.TransactionOption;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link LocalDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLLocalDatabaseSession extends MySQLDatabaseSession<LocalDatabaseSession> implements LocalDatabaseSession {


    static LocalDatabaseSession create(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLLocalDatabaseSession(factory, protocol);
    }

    static PoolLocalDatabaseSession forPoolVendor(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLPoolLocalDatabaseSession(factory, protocol);
    }

    /**
     * private constructor
     */
    private MySQLLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Mono<LocalDatabaseSession> startTransaction(TransactionOption option) {
        return this.protocol.startTransaction(option)
                .thenReturn(this);
    }

    @Override
    public final boolean inTransaction() {
        return this.protocol.inTransaction();
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
     * This class is implementation of {@link PoolLocalDatabaseSession} with MySQL client protocol.
     * </p>
     *
     * @since 1.0
     */
    private static final class MySQLPoolLocalDatabaseSession extends MySQLLocalDatabaseSession
            implements PoolLocalDatabaseSession {

        private MySQLPoolLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
            super(factory, protocol);
        }


        @Override
        public Publisher<PoolLocalDatabaseSession> reconnect(int maxReconnect) {
            return this.protocol.reconnect(maxReconnect)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> ping(int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }// MySQLPoolLocalDatabaseSession


}
