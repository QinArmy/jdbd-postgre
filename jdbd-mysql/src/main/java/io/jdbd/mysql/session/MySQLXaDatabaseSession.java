package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.pool.PoolXaDatabaseSession;
import io.jdbd.session.XaDatabaseSession;
import io.jdbd.session.Xid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link XaDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLXaDatabaseSession extends MySQLDatabaseSession implements XaDatabaseSession {

    static XaDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLXaDatabaseSession(adjutant, protocol);
    }

    static PoolXaDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new MySQLPoolXaDatabaseSession(adjutant, protocol);
    }

    private MySQLXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }


    @Override
    public final Mono<XaDatabaseSession> start(Xid xid, final int flags) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> commit(Xid xid, final boolean onePhase) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> end(final Xid xid, final int flags) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> forget(final Xid xid) {
        return null;
    }

    @Override
    public final Mono<Integer> prepare(final Xid xid) {
        return null;
    }

    @Override
    public final Flux<Xid> recover(final int flag) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> rollback(final Xid xid) {
        return null;
    }

    /**
     * <p>
     * This class is implementation of {@link PoolXaDatabaseSession} with MySQL client protocol.
     * </p>
     */
    private static final class MySQLPoolXaDatabaseSession extends MySQLXaDatabaseSession
            implements PoolXaDatabaseSession {

        private MySQLPoolXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolXaDatabaseSession> ping(int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolXaDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }

}
