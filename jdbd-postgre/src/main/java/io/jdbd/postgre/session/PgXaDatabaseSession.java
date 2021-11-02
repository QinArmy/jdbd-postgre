package io.jdbd.postgre.session;

import io.jdbd.pool.PoolXaDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.session.XaDatabaseSession;
import io.jdbd.session.Xid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link XaDatabaseSession} with Postgre client protocol.
 * </p>
 */
class PgXaDatabaseSession extends PgDatabaseSession implements XaDatabaseSession {


    static PgXaDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgXaDatabaseSession(adjutant, protocol);
    }

    static PgPoolXaDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgPoolXaDatabaseSession(adjutant, protocol);
    }


    private PgXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }


    @Override
    public final Mono<XaDatabaseSession> start(Xid xid, int flags) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> commit(Xid xid, boolean onePhase) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> end(Xid xid, int flags) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> forget(Xid xid) {
        return null;
    }

    @Override
    public final Mono<Integer> prepare(Xid xid) {
        return null;
    }

    @Override
    public final Flux<Xid> recover(int flags) {
        return null;
    }

    @Override
    public final Mono<XaDatabaseSession> rollback(Xid xid) {
        return null;
    }


    private static final class PgPoolXaDatabaseSession extends PgXaDatabaseSession implements PoolXaDatabaseSession {

        private PgPoolXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolXaDatabaseSession> ping(final int timeoutSeconds) {
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
