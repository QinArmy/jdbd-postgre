package io.jdbd.postgre.session;

import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.RmDatabaseSession;
import io.jdbd.session.Xid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with Postgre client protocol.
 * </p>
 */
class PgRmDatabaseSession extends PgDatabaseSession implements RmDatabaseSession {


    static PgRmDatabaseSession create(SessionAdjutant adjutant, PgProtocol protocol) {
        return new PgRmDatabaseSession(adjutant, protocol);
    }

    static PgPoolXaDatabaseSession forPoolVendor(SessionAdjutant adjutant, PgProtocol protocol) {
        return new PgPoolXaDatabaseSession(adjutant, protocol);
    }


    private PgRmDatabaseSession(SessionAdjutant adjutant, PgProtocol protocol) {
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


    private static final class PgPoolXaDatabaseSession extends PgRmDatabaseSession implements PoolRmDatabaseSession {

        private PgPoolXaDatabaseSession(SessionAdjutant adjutant, PgProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolRmDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolRmDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }

}
