package io.jdbd.postgre.session;

import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.*;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.Warning;
import io.jdbd.session.*;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdBuffers;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.XaUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with Postgre client protocol.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-prepare-transaction.html">PREPARE TRANSACTION</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-commit-prepared.html">COMMIT PREPARED</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-rollback-prepared.html">ROLLBACK PREPARED</a>
 * @since 1.0
 */
class PgRmDatabaseSession extends PgDatabaseSession<RmDatabaseSession> implements RmDatabaseSession {


    static PgRmDatabaseSession create(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgRmDatabaseSession(factory, protocol);
    }

    static PgPoolRmDatabaseSession forPoolVendor(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgPoolRmDatabaseSession(factory, protocol);
    }

    private static final AtomicReferenceFieldUpdater<PgRmDatabaseSession, XaStatesTriple> CURRENT_TRIPLE = AtomicReferenceFieldUpdater
            .newUpdater(PgRmDatabaseSession.class, XaStatesTriple.class, "currentTriple");

    private final ConcurrentMap<Xid, XaStatesTriple> preparedXaMp = PgCollections.concurrentHashMap();

    private volatile XaStatesTriple currentTriple = null;


    private PgRmDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        final XaStatesTriple currentXaPair = this.currentTriple;
        return this.protocol.transactionStatus()
                .map(status -> mapTransactionStatus(status, currentXaPair));
    }

    @Override
    public final boolean isSupportForget() {
        //always false, postgre don't support
        return false;
    }

    @Override
    public final int startSupportFlags() {
        return TM_NO_FLAGS;
    }

    @Override
    public final int endSupportFlags() {
        return (TM_SUCCESS | TM_FAIL);
    }

    @Override
    public final int recoverSupportFlags() {
        return (TM_START_RSCAN | TM_END_RSCAN | TM_NO_FLAGS);
    }

    @Override
    public final Publisher<RmDatabaseSession> start(Xid xid, int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }

    @Override
    public final Publisher<RmDatabaseSession> start(final Xid xid, final int flags,
                                                    final TransactionOption option) {
        final XaStatesTriple triple = this.currentTriple;

        final XaException error;
        final Mono<RmDatabaseSession> mono;
        if (triple != null || this.protocol.inTransaction()) {
            mono = Mono.error(new XaException("session is busy with another transaction", XaException.XAER_PROTO));
        } else if ((error = XaUtils.checkXid(xid)) != null) {
            mono = Mono.error(error);
        } else if ((flags & TM_RESUME) != 0) {
            mono = Mono.error(JdbdExceptions.xaDontSupportSuspendResume());
        } else if ((flags & TM_JOIN) != 0) {
            mono = Mono.error(new XaException("join not implemented", XaException.XAER_RMERR));
        } else if (flags != TM_NO_FLAGS) {
            mono = Mono.error(PgExceptions.xaInvalidFlagForStart(flags));
        } else {
            mono = this.protocol.startTransaction(option, HandleMode.ERROR_IF_EXISTS)
                    .onErrorMap(this::handleStartError)
                    .flatMap(states -> handleStartResult(states, xid));
        }
        return mono;
    }


    @Override
    public final Publisher<RmDatabaseSession> end(Xid xid, int flags) {
        return this.end(xid, flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags, final Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.currentTriple;

        final Mono<RmDatabaseSession> mono;
        if (triple == null || !Objects.equals(xid, triple.xid)) {
            mono = Mono.error(PgExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (triple.xaStates != XaStates.ACTIVE) {
            mono = Mono.error(PgExceptions.xaTransactionDontSupportEndCommand(triple.xid, triple.xaStates));
        } else if ((flags & TM_RESUME) != 0) {
            mono = Mono.error(PgExceptions.xaDontSupportSuspendResume());
        } else if (((~(TM_SUCCESS | TM_FAIL)) & flags) != 0) {
            mono = Mono.error(PgExceptions.xaInvalidFlagForEnd(flags));
        } else if (!PgCollections.isEmpty(optionMap)) {
            mono = Mono.error(PgExceptions.xaDontSupportOptionMap("end", optionMap));
        } else if (CURRENT_TRIPLE.compareAndSet(this, triple, new XaStatesTriple(triple.xid, XaStates.IDLE, flags))) {
            mono = Mono.just(this);
        } else {
            mono = Mono.error(sessionBusyWithThread(triple.xid));
        }
        return mono;
    }


    @Override
    public final Publisher<Integer> prepare(Xid xid) {
        return this.prepare(xid, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-prepare-transaction.html">PREPARE TRANSACTION</a>
     */
    @Override
    public final Publisher<Integer> prepare(final Xid xid, final Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple = this.currentTriple;

        final Mono<Integer> mono;
        if (triple == null || !Objects.equals(xid, triple.xid)) {
            mono = Mono.error(PgExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (triple.xaStates != XaStates.IDLE) {
            mono = Mono.error(PgExceptions.xaStatesDontSupportPrepareCommand(triple.xid, triple.xaStates));
        } else if (!PgCollections.isEmpty(optionMap)) {
            mono = Mono.error(PgExceptions.xaDontSupportOptionMap("prepare", optionMap));
        } else {
            final StringBuilder builder = new StringBuilder(60);
            builder.append("PREPARE TRANSACTION  ");
            xidToString(builder, triple.xid); // must use triple.xid

            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .onErrorMap(error -> handlePrepareError(triple, error))
                    .flatMap(states -> handlePrepareResult(states, triple));
        }
        return mono;
    }


    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase) {
        return this.commit(xid, onePhase, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-commit-prepared.html">COMMIT PREPARED</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> commit(final @Nullable Xid xid, final boolean onePhase,
                                                     final Map<Option<?>, ?> optionMap) {
        if (xid == null) {
            return Mono.error(PgExceptions.xidIsNull());
        }
        final XaStatesTriple triple;
        if (onePhase) {
            triple = this.currentTriple;
        } else {
            triple = this.preparedXaMp.get(xid);
        }

        final Mono<RmDatabaseSession> mono;
        if (triple == null) {
            mono = Mono.error(PgExceptions.xaUnknownTransaction(xid));
        } else if (!Objects.equals(xid, triple.xid)) {
            mono = Mono.error(PgExceptions.xaNonCurrentTransaction(xid));
        } else if ((triple.flags & TM_FAIL) != 0) {
            mono = Mono.error(PgExceptions.xaTransactionRollbackOnly(triple.xid));
        } else if (onePhase) {
            if (triple.xaStates == XaStates.IDLE) {
                mono = this.protocol.commit(optionMap)
                        .flatMap(states -> handleOnePhaseCommitResult(states, triple));
            } else {
                mono = Mono.error(PgExceptions.xaStatesDontSupportCommitCommand(triple.xid, triple.xaStates));
            }
        } else if (triple.xaStates == XaStates.PREPARED) {
            final StringBuilder builder = new StringBuilder(64);
            builder.append("COMMIT PREPARED ");
            xidToString(builder, triple.xid); // must use triple.xid

            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .flatMap(states -> handleCommitOrRollbackResult(triple));
        } else {
            mono = Mono.error(PgExceptions.xaStatesDontSupportCommitCommand(triple.xid, triple.xaStates));
        }
        return mono;
    }


    @Override
    public final Publisher<RmDatabaseSession> rollback(Xid xid) {
        return this.rollback(xid, Collections.emptyMap());
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-rollback-prepared.html">ROLLBACK PREPARED</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> rollback(final @Nullable Xid xid, final Map<Option<?>, ?> optionMap) {
        final XaStatesTriple triple;

        final Mono<RmDatabaseSession> mono;
        if (xid == null) {
            mono = Mono.error(PgExceptions.xidIsNull());
        } else if ((triple = this.preparedXaMp.get(xid)) == null) {
            String m = String.format("xa %s isn't prepared transaction.", xid);
            mono = Mono.error(new XaException(m, XaException.XAER_PROTO));
        } else {
            final StringBuilder builder = new StringBuilder(80);
            builder.append("ROLLBACK PREPARED ");
            xidToString(builder, triple.xid); // must use triple.xid

            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .flatMap(states -> handleCommitOrRollbackResult(triple));
        }
        return mono;
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
    public final Publisher<Optional<Xid>> recover(int flags) {
        return this.recover(flags, Collections.emptyMap());
    }

    @Override
    public final Publisher<Optional<Xid>> recover(final int flags, Map<Option<?>, ?> optionMap) {
        final int supportBitSet = recoverSupportFlags();

        final Flux<Optional<Xid>> flux;
        if (((~supportBitSet) & flags) != 0) {
            flux = Flux.error(PgExceptions.xaInvalidFlagForRecover(flags));
        } else if ((flags & TM_START_RSCAN) == 0) {
            flux = Flux.empty();
        } else {
            final String sql = "SELECT gid FROM pg_prepared_xacts AS t WHERE t.database = current_database()";
            flux = this.protocol.query(Stmts.stmt(sql), this::mapXid);
        }
        return flux;
    }


    /**
     * @see #commit(Xid, boolean, Map)
     * @see #rollback(Xid, Map)
     */
    private Mono<RmDatabaseSession> handleCommitOrRollbackResult(final XaStatesTriple old) {
        final Mono<RmDatabaseSession> mono;
        if (this.preparedXaMp.remove(old.xid, old)) {
            mono = Mono.just(this);
        } else {
            mono = Mono.error(sessionBusyWithThread(old.xid));
        }
        return mono;
    }


    /**
     * @see #commit(Xid, boolean, Map)
     * @see #rollback(Xid, Map)
     */
    private Mono<Xid> validaXidFromDatabase(final Xid xid) {
        final StringBuilder builder = new StringBuilder(128);
        builder.append("SELECT t.gid FROM pg_prepared_xacts AS t WHERE t.database = current_database() AND t.gid = ");
        xidToString(builder, xid);
        return this.protocol.query(Stmts.stmt(builder.toString()), this::mapXid)
                .elementAt(0)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .switchIfEmpty(Mono.defer(() -> Mono.error(PgExceptions.xaUnknownTransaction(xid))));
    }


    /**
     * @see #start(Xid, int, TransactionOption)
     */
    private Mono<RmDatabaseSession> handleStartResult(final ResultStates states, final Xid xid) {
        final Warning warning;
        warning = states.warning();

        final Mono<RmDatabaseSession> mono;
        if (warning != null && PgSQLStates.ACTIVE_SQL_TRANSACTION.equals(warning.valueOf(Option.SQL_STATE))) {
            final XaException error;
            error = new XaException(warning.message(), PgSQLStates.ACTIVE_SQL_TRANSACTION, 0, XaException.XAER_PROTO);
            mono = Mono.error(error);
        } else if (CURRENT_TRIPLE.compareAndSet(this, null, new XaStatesTriple(xid, XaStates.ACTIVE, TM_NO_FLAGS))) {
            mono = Mono.just(this);
        } else {
            mono = Mono.error(sessionBusyWithThread(xid));
        }
        return mono;
    }


    /**
     * @see #prepare(Xid, Map)
     */
    private Mono<Integer> handlePrepareResult(final ResultStates states, final XaStatesTriple old) {
        final Warning warning;
        warning = states.warning();

        final Mono<Integer> mono;
        if (warning != null && PgSQLStates.PG_NO_ACTIVE_SQL_TRANSACTION.equals(warning.valueOf(Option.SQL_STATE))) {
            final XaException e;
            e = new XaException(warning.message(), PgSQLStates.PG_NO_ACTIVE_SQL_TRANSACTION, 0, XaException.XAER_PROTO);
            mono = Mono.error(e);
        } else if (!CURRENT_TRIPLE.compareAndSet(this, old, null)
                || this.preparedXaMp.putIfAbsent(old.xid, new XaStatesTriple(old.xid, XaStates.PREPARED, old.flags)) != null) {
            mono = Mono.error(sessionBusyWithThread(old.xid));
        } else {
            mono = Mono.just(XA_OK);
        }
        return mono;
    }

    /**
     * @see #commit(Xid, boolean, Map)
     */
    private Mono<RmDatabaseSession> handleOnePhaseCommitResult(final ResultStates states, final XaStatesTriple old) {
        final Warning warning;
        warning = states.warning();

        final Mono<RmDatabaseSession> mono;
        if (warning != null && PgSQLStates.PG_NO_ACTIVE_SQL_TRANSACTION.equals(warning.valueOf(Option.SQL_STATE))) {
            // here , application use sql command commit/rollback transaction
            final XaException e;
            e = new XaException(warning.message(), PgSQLStates.PG_NO_ACTIVE_SQL_TRANSACTION, 0, XaException.XAER_RMERR);
            mono = Mono.error(e);
        } else if (CURRENT_TRIPLE.compareAndSet(this, old, null)) {
            mono = Mono.just(this);
        } else {
            mono = Mono.error(sessionBusyWithThread(old.xid));
        }
        return mono;
    }


    /**
     * @see #transactionStatus()
     */
    private TransactionStatus mapTransactionStatus(final TransactionStatus status,
                                                   final @Nullable XaStatesTriple currentTriple) {
        final TransactionStatus finalStatus;
        if (currentTriple == null) {
            finalStatus = status;
        } else {
            finalStatus = new PgXaTransactionStatus(currentTriple, status);
        }
        return finalStatus;
    }

    /**
     * @see #recover(int, Map)
     * @see #xidToString(StringBuilder, Xid)
     */
    private Optional<Xid> mapXid(final CurrentRow row) {
        final String string, gtridHex, bqualHex;
        string = row.getNonNull(0, String.class);

        final int hyphenIndex, poundIndex;
        hyphenIndex = string.indexOf('-');
        poundIndex = string.lastIndexOf('#');

        final int length;
        length = string.length();
        if (hyphenIndex == 0 || poundIndex < 0 || poundIndex + 1 == length) {
            // isn't this driver create xid
            return Optional.empty();
        }

        if (hyphenIndex < 0) {
            gtridHex = string.substring(0, poundIndex);
            bqualHex = null;
        } else {
            gtridHex = string.substring(0, hyphenIndex);
            bqualHex = string.substring(hyphenIndex + 1, poundIndex);
        }

        Optional<Xid> optional;
        try {
            final int formatId;
            formatId = Integer.parseInt(string.substring(poundIndex + 1));

            final String gtrid, bqual;

            byte[] bytes;
            bytes = gtridHex.getBytes(StandardCharsets.UTF_8);
            gtrid = new String(PgBuffers.decodeHex(bytes, bytes.length), StandardCharsets.UTF_8);

            if (bqualHex == null) {
                bqual = null;
            } else {
                bytes = bqualHex.getBytes(StandardCharsets.UTF_8);
                bqual = new String(PgBuffers.decodeHex(bytes, bytes.length), StandardCharsets.UTF_8);
            }
            optional = Optional.of(Xid.from(gtrid, bqual, formatId));
        } catch (Throwable e) {
            // isn't this driver create xid
            optional = Optional.empty();
        }
        return optional;
    }


    /**
     * @see #start(Xid, int, TransactionOption)
     */
    private XaException handleStartError(Throwable cause) {
        if (cause instanceof XaException) {
            return (XaException) cause;
        }
        return new XaException("start xa transaction occur error.", cause, XaException.XAER_RMERR);
    }

    /**
     * @see #prepare(Xid, Map)
     */
    private XaException handlePrepareError(XaStatesTriple triple, Throwable cause) {
        if (cause instanceof XaException) {
            return (XaException) cause;
        }
        String m = String.format("prepare xa transaction %s occur error.", triple);
        return new XaException(m, cause, XaException.XAER_RMERR);
    }

    /**
     * @see #end(Xid, int, Map)
     */
    private static XaException sessionBusyWithThread(@Nullable Xid xid) {
        String m = String.format("session xid[%s] is busy with another thread", xid);
        return new XaException(m, XaException.XAER_PROTO);
    }

    /**
     * @see #prepare(Xid, Map)
     * @see #mapXid(CurrentRow)
     */
    private static String xidToString(final StringBuilder builder, final Xid xid) {
        builder.append(PgConstant.QUOTE);
        byte[] bytes;
        bytes = xid.getGtrid().getBytes(StandardCharsets.UTF_8);

        builder.append(new String(JdbdBuffers.hexEscapes(true, bytes, bytes.length), StandardCharsets.UTF_8));

        final String bqual;
        bqual = xid.getBqual();
        if (bqual != null) {
            bytes = bqual.getBytes(StandardCharsets.UTF_8);
            builder.append('-') // avoid to same with jdbc-postgre
                    .append(new String(JdbdBuffers.hexEscapes(true, bytes, bytes.length), StandardCharsets.UTF_8));
        }

        return builder.append('#') // avoid to same with jdbc-postgre
                .append(xid.getFormatId())
                .append(PgConstant.QUOTE)
                .toString();
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

        private XaStatesTriple(Xid xid, XaStates xaStates, int flags) {
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

        @Override
        public String toString() {
            return PgStrings.builder()
                    .append(getClass().getName())
                    .append(" [ xid : ")
                    .append(this.xid)
                    .append(" , xa states : ")
                    .append(this.xaStates.name())
                    .append(" , flags : ")
                    .append(Integer.toBinaryString(this.flags))
                    .append(" ]")
                    .toString();
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
            final XaStatesTriple triple = this.triple;
            final T value;
            if (option == Option.XA_STATES) {
                value = (T) triple.xaStates;
            } else if (option == Option.XID) {
                value = (T) triple.xid;
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
