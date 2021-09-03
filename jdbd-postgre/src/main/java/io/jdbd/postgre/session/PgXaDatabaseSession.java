package io.jdbd.postgre.session;

import io.jdbd.pool.PoolXaDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.xa.XaDatabaseSession;
import reactor.core.publisher.Mono;

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


    private static final class PgPoolXaDatabaseSession extends PgXaDatabaseSession implements PoolXaDatabaseSession {

        private PgPoolXaDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public final Mono<PoolXaDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public final Mono<PoolXaDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }

}
