package io.jdbd.postgre.session;

import io.jdbd.pool.PoolGlobalDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.session.RmDatabaseSession;
import io.jdbd.session.Xid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with Postgre client protocol.
 * </p>
 */
class PgXaDatabaseSession extends PgDatabaseSession implements RmDatabaseSession {


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
    public final Mono<RmDatabaseSession> start(Xid xid, int flags) {
        return null;
    }

    @Override
    public final Mono<RmDatabaseSession> commit(Xid xid, boolean onePhase) {
        return null;
    }

    @Override
    public final Mono<RmDatabaseSession> end(Xid xid, int flags) {
        return null;
    }

    @Override
    public final Mono<RmDatabaseSession> forget(Xid xid) {
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
    public final Mono<RmDatabaseSession> rollback(Xid xid) {
        return null;
    }


    private static final class PgPoolXaDatabaseSession extends PgXaDatabaseSession implements PoolGlobalDatabaseSession {

        private PgPoolXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolGlobalDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolGlobalDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }

}
