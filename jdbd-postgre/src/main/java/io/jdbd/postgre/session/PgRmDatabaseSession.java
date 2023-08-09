package io.jdbd.postgre.session;

import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdExceptions;
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
 *
 * @since 1.0
 */
class PgRmDatabaseSession extends PgDatabaseSession<RmDatabaseSession> implements RmDatabaseSession {


    static PgRmDatabaseSession create(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgRmDatabaseSession(factory, protocol);
    }

    static PgPoolRmDatabaseSession forPoolVendor(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgPoolRmDatabaseSession(factory, protocol);
    }

    private static final AtomicReferenceFieldUpdater<PgRmDatabaseSession, XaStatesTriple> XA_TRIPLE = AtomicReferenceFieldUpdater
            .newUpdater(PgRmDatabaseSession.class, XaStatesTriple.class, "xaTriple");


    private static final XaStatesTriple DEFAULT_XA_TRIPLE = new XaStatesTriple(null, XaStates.NONE, 0);
    private volatile XaStatesTriple xaTriple = DEFAULT_XA_TRIPLE;


    private PgRmDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        final XaStatesTriple currentXaPair = this.xaTriple;
        return this.protocol.transactionStatus()
                .map(status -> this.mapTransactionStatus(status, currentXaPair));
    }


    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }

    @Override
    public final Publisher<RmDatabaseSession> start(final @Nullable Xid xid, final int flags,
                                                    final TransactionOption option) {
        final XaStatesTriple triple = this.xaTriple;

        final XaException error;
        if (triple != DEFAULT_XA_TRIPLE || this.protocol.inTransaction()) {
            error = new XaException("session is busy with another transaction", XaException.XAER_PROTO);
        } else if (xid == null) {
            error = PgExceptions.xidIsNull();
        } else if ((flags & TM_RESUME) != 0) {
            error = JdbdExceptions.xaDontSupportSuspendResume();
        } else if ((flags & TM_JOIN) != 0) {
            error = new XaException("join not implemented", XaException.XAER_RMERR);
        } else if (flags != TM_NO_FLAGS) {
            error = PgExceptions.xaInvalidFlagForStart(flags);
        } else if (XA_TRIPLE.compareAndSet(this, DEFAULT_XA_TRIPLE, new XaStatesTriple(xid, XaStates.STARTED, TM_NO_FLAGS))) {
            error = null;
        } else {
            error = new XaException("session is busy with another transaction", XaException.XAER_PROTO);
        }
        if (error != null) {
            return Mono.error(error);
        }
        return this.protocol.startTransaction(option, HandleMode.ERROR_IF_EXISTS)
                .onErrorMap(this::onStartLocalTransactionError)
                .thenReturn(this);
    }


    @Override
    public final Publisher<RmDatabaseSession> end(Xid xid, int flags) {
        return this.end(xid, flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags, final Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.xaTriple;

        final XaException error;
        if (triple == DEFAULT_XA_TRIPLE || !Objects.equals(xid, triple.xid)) {
            error = PgExceptions.xaTransactionNotStart(triple.xid);
        } else if (triple.xaStates != XaStates.STARTED) {
            error = PgExceptions.xaTransactionDontSupportEndCommand(triple.xid, triple.xaStates);
        } else if ((flags & TM_RESUME) != 0) {
            error = JdbdExceptions.xaDontSupportSuspendResume();
        } else if (((~(TM_SUCCESS | TM_FAIL)) & flags) != 0) {
            error = PgExceptions.xaInvalidFlagForEnd(flags);
        } else if (XA_TRIPLE.compareAndSet(this, triple, new XaStatesTriple(triple.xid, XaStates.ENDED, flags))) {
            error = null;
        } else {
            error = sessionBusyWithThread(triple.xid);
        }
        if (error != null) {
            return Mono.error(error);
        }
        return Mono.just(this);
    }


    @Override
    public final Publisher<Integer> prepare(Xid xid) {
        return this.prepare(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<Integer> prepare(final Xid xid, final Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.xaTriple;

        final XaException error;
        if (triple == DEFAULT_XA_TRIPLE || !Objects.equals(xid, triple.xid)) {
            error = PgExceptions.xaTransactionNotStart(triple.xid);
        } else if (triple.xaStates != XaStates.ENDED) {
            error = PgExceptions.xaTransactionDontSupportPrepareCommand(triple.xid, triple.xaStates);
        } else {
            error = null;
        }
        if (error != null) {
            return Mono.error(error);
        }
        final StringBuilder builder = new StringBuilder();

        builder.append("PREPARE TRANSACTION ");
        this.protocol.bindIdentifier(builder, xidToString(triple.xid));// must use triple.xid
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .then(Mono.defer(() -> handlePrepareSuccess(triple)));
    }


    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase) {
        return this.commit(xid, onePhase, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(final Xid xid, final boolean onePhase, Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.xaTriple;

        final XaException error;
        if (triple == DEFAULT_XA_TRIPLE || !Objects.equals(xid, triple.xid)) {
            error = PgExceptions.xaTransactionNotStart(triple.xid);
        } else if ((triple.flags & TM_FAIL) != 0) {
            error = PgExceptions.xaTransactionRollbackOnly(triple.xid);
        } else if (triple.xaStates != XaStates.PREPARED) {
            error = PgExceptions.xaTransactionDontSupportCommitCommand(triple.xid, triple.xaStates);
        } else {
            error = null;
        }

        if (error != null) {
            return Mono.error(error);
        }

        final Mono<ResultStates> mono;
        if (onePhase) {
            mono = this.protocol.commit(optionMap);
        } else {
            final StringBuilder builder = new StringBuilder(80);
            builder.append("COMMIT PREPARED ");
            this.protocol.bindIdentifier(builder, xidToString(triple.xid)); // must use triple.xid
            mono = this.protocol.update(Stmts.stmt(builder.toString()));
        }
        return mono.then(Mono.defer(() -> handleCommitOrRollbackSuccess(triple)));
    }

    @Override
    public final Publisher<RmDatabaseSession> rollback(Xid xid) {
        return this.rollback(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> rollback(final Xid xid, Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.xaTriple;

        final XaException error;
        if (triple == DEFAULT_XA_TRIPLE || !Objects.equals(xid, triple.xid)) {
            error = PgExceptions.xaTransactionNotStart(triple.xid);
        } else if (triple.xaStates != XaStates.PREPARED) {
            error = PgExceptions.xaTransactionDontSupportRollbackCommand(triple.xid, triple.xaStates);
        } else {
            error = null;
        }
        if (error != null) {
            return Mono.error(error);
        }
        final StringBuilder builder = new StringBuilder(80);
        builder.append("ROLLBACK PREPARED ");
        this.protocol.bindIdentifier(builder, xidToString(triple.xid));// must use triple.xid
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .then(Mono.defer(() -> handleCommitOrRollbackSuccess(triple)));
    }

    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid) {
        return this.forget(xid, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> forget(final Xid xid, final Map<Option<?>, ?> optionMap) {
        return Mono.error(new XaException("postgre don't support forget command", XaException.XAER_RMERR));
    }

    @Override
    public final Publisher<Xid> recover(int flags) {
        return this.recover(flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<Xid> recover(int flags, Map<Option<?>, ?> optionMap) {
        return null;
    }


    /**
     * @see #prepare(Xid, Map)
     */
    private Mono<Integer> handlePrepareSuccess(final XaStatesTriple old) {
        final Mono<Integer> mono;
        if (XA_TRIPLE.compareAndSet(this, old, new XaStatesTriple(old.xid, XaStates.PREPARED, old.flags))) {
            mono = Mono.just(XA_OK);
        } else {
            mono = Mono.error(sessionBusyWithThread(old.xid));
        }
        return mono;
    }

    private Mono<RmDatabaseSession> handleCommitOrRollbackSuccess(final XaStatesTriple old) {
        final Mono<RmDatabaseSession> mono;
        if (XA_TRIPLE.compareAndSet(this, old, DEFAULT_XA_TRIPLE)) {
            mono = Mono.just(this);
        } else {
            mono = Mono.error(sessionBusyWithThread(old.xid));
        }
        return mono;
    }

    /**
     * @see #transactionStatus()
     */
    private TransactionStatus mapTransactionStatus(TransactionStatus status, XaStatesTriple currentXaPair) {
        final TransactionStatus transactionStatus;
        if (currentXaPair == DEFAULT_XA_TRIPLE) {
            transactionStatus = status;
        } else {
            transactionStatus = new PgXaTransactionStatus(currentXaPair, status);
        }
        return transactionStatus;
    }

    private XaException onStartLocalTransactionError(Throwable cause) {
        return new XaException("start xa transaction occur error.", cause, XaException.XAER_RMERR);
    }

    /**
     * @see #end(Xid, int, Map)
     */
    private static XaException sessionBusyWithThread(@Nullable Xid xid) {
        String m = String.format("session xid[%s] is busy with another thread", xid);
        return new XaException(m, XaException.XAER_PROTO);
    }

    private static String xidToString(final Xid xid) {

        final StringBuilder builder = new StringBuilder(64);
        builder.append(xid.getFormatId())
                .append('_')
                .append(xid.getGtrid());

        final String bqual;
        bqual = xid.getBqual();
        if (bqual != null) {
            builder.append('_')
                    .append(xid.getBqual());
        }
        return builder.toString();
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

    private static final class XaStatesTriple {

        private final Xid xid;

        private final XaStates xaStates;

        private final int flags;

        private XaStatesTriple(@Nullable Xid xid, XaStates xaStates, int flags) {
            this.xid = xid;
            this.xaStates = xaStates;
            this.flags = flags;
        }


        @Override
        public int hashCode() {
            return Objects.hash(this.xid, this.xaStates, this.flags);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof XaStatesTriple) {
                final XaStatesTriple o = (XaStatesTriple) obj;
                match = o.xaStates == this.xaStates
                        && o.flags == this.flags
                        && Objects.equals(o.xid, this.xid);
            } else {
                match = false;
            }
            return match;
        }


    }// XaStatesPair

    private static final class PgXaTransactionStatus implements TransactionStatus {

        private final XaStatesTriple triple;

        private final TransactionStatus status;


        private PgXaTransactionStatus(XaStatesTriple triple, TransactionStatus status) {
            this.triple = triple;
            this.status = status;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T valueOf(final Option<T> option) {
            final XaStatesTriple pair = this.triple;
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
            //TODO how ?  after prepare
            return this.status.inTransaction();
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.triple, this.status);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof PgXaTransactionStatus) {
                final PgXaTransactionStatus o = (PgXaTransactionStatus) obj;
                match = o.triple.equals(this.triple) && o.status.equals(this.status);
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
                    .append(this.triple.xid)
                    .append(" , xa states : ")
                    .append(this.triple.xaStates)
                    .append(", flags : ")
                    .append(Integer.toBinaryString(this.triple.flags))
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
