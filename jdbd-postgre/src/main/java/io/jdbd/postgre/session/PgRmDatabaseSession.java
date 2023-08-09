package io.jdbd.postgre.session;

import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.session.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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

    private static final AtomicReferenceFieldUpdater<PgRmDatabaseSession, XaStatesPair> XA_PAIR = AtomicReferenceFieldUpdater
            .newUpdater(PgRmDatabaseSession.class, XaStatesPair.class, "xaPair");


    private static final XaStatesPair DEFAULT_XA_PAIR = new XaStatesPair(null, XaStates.NONE);
    private volatile XaStatesPair xaPair = DEFAULT_XA_PAIR;

    private PgRmDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        final XaStatesPair currentXaPair = this.xaPair;
        return this.protocol.transactionStatus()
                .map(status -> {
                    TransactionStatus transactionStatus;
                    if (currentXaPair == DEFAULT_XA_PAIR) {
                        transactionStatus = status;
                    } else {
                        transactionStatus = new PgXaTransactionStatus(currentXaPair, status);
                    }
                    return transactionStatus;
                });
    }


    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }

    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags, TransactionOption option) {
        final XaStatesPair currentXaPair = this.xaPair;
        if (currentXaPair != DEFAULT_XA_PAIR) {
            return Mono.error(new XaException());
        }

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

    private static final class XaStatesPair {

        private final Xid xid;

        private final XaStates xaStates;

        private XaStatesPair(@Nullable Xid xid, XaStates xaStates) {
            this.xid = xid;
            this.xaStates = xaStates;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.xid, this.xaStates);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof XaStatesPair) {
                final XaStatesPair o = (XaStatesPair) obj;
                match = o.xaStates == this.xaStates && Objects.equals(o.xid, this.xid);
            } else {
                match = false;
            }
            return match;
        }


    }// XaStatesPair

    private static final class PgXaTransactionStatus implements TransactionStatus {

        private final XaStatesPair pair;

        private final TransactionStatus status;


        private PgXaTransactionStatus(XaStatesPair pair, TransactionStatus status) {
            this.pair = pair;
            this.status = status;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T valueOf(final Option<T> option) {
            final XaStatesPair pair = this.pair;
            final T value;
            if (option == Option.XA_STATES) {
                value = (T) pair.xaStates;
            } else if (option == Option.XID) {
                value = (T) pair.xid;
            } else {
                value = this.status.valueOf(option);
            }
            return value;
        }

        @Override
        public boolean isReadOnly() {
            return this.status.isReadOnly();
        }

        @NonNull
        @Override
        public Isolation isolation() {
            return this.status.isolation();
        }

        @Override
        public boolean inTransaction() {
            return this.status.inTransaction();
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.pair, this.status);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof PgXaTransactionStatus) {
                final PgXaTransactionStatus o = (PgXaTransactionStatus) obj;
                match = o.pair.equals(this.pair) && o.status.equals(this.status);
            } else {
                match = false;
            }
            return match;
        }

        @Override
        public String toString() {
            return PgStrings.builder()
                    .append(getClass().getName())
                    .append("[ xid : ")
                    .append(this.pair.xid)
                    .append(" , xa states : ")
                    .append(this.pair.xaStates)
                    .append(" , isolation : ")
                    .append(this.status.isolation())
                    .append(" , read only : ")
                    .append(this.status.isReadOnly())
                    .append(" , in transaction : ")
                    .append(this.status.inTransaction())
                    .append(" ]")
                    .toString();
        }


    }//PgTransactionStatus


}
