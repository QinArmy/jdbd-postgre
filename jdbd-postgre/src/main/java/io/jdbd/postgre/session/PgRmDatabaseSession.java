package io.jdbd.postgre.session;

import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.Option;
import io.jdbd.session.RmDatabaseSession;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.Xid;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with Postgre client protocol.
 * </p>
 */
class PgRmDatabaseSession extends PgDatabaseSession<RmDatabaseSession> implements RmDatabaseSession {


    static PgRmDatabaseSession create(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgRmDatabaseSession(factory, protocol);
    }

    static PgPoolRmDatabaseSession forPoolVendor(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgPoolRmDatabaseSession(factory, protocol);
    }


    private PgRmDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option) {
        return this.protocol.setTransactionOption(option, Collections.emptyMap())
                .thenReturn(this);
    }

    @Override
    public final Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option, Map<Option<?>, ?> optionMap) {
        return this.protocol.setTransactionOption(option, optionMap)
                .thenReturn(this);
    }

    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags) {
        return this.start(xid, flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<RmDatabaseSession> end(Xid xid, int flags) {
        return this.end(xid, flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> end(Xid xid, int flags, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<Integer> prepare(Xid xid) {
        return this.prepare(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<Integer> prepare(Xid xid, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase) {
        return this.commit(xid, onePhase, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<RmDatabaseSession> rollback(Xid xid) {
        return this.rollback(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> rollback(Xid xid, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid) {
        return this.forget(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid, Map<Option<?>, ?> optionMap) {
        return null;
    }

    @Override
    public final Publisher<Xid> recover(int flags) {
        return this.recover(flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<Xid> recover(int flags, Map<Option<?>, ?> optionMap) {
        return null;
    }

    private static final class PgPoolRmDatabaseSession extends PgRmDatabaseSession implements PoolRmDatabaseSession {


        private PgPoolRmDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
            super(factory, protocol);
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

        @Override
        public Publisher<PoolRmDatabaseSession> reconnect() {
            return this.protocol.reconnect()
                    .thenReturn(this);
        }


    }// PgPoolXaDatabaseSession


}
