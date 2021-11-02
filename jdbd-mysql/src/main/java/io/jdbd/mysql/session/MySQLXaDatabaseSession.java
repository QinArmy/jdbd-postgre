package io.jdbd.mysql.session;


import io.jdbd.JdbdXaException;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLBuffers;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.pool.PoolXaDatabaseSession;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.XaDatabaseSession;
import io.jdbd.session.Xid;
import io.jdbd.vendor.session.XidImpl;
import io.jdbd.vendor.util.JdbdExceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

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
    public final Mono<XaDatabaseSession> start(final Xid xid, final int flags) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA START");
        try {
            appendXid(builder, xid);
            switch (flags) {
                case TMJOIN:
                    builder.append(" JOIN");
                    break;
                case TMRESUME:
                    builder.append(" RESUME");
                    break;
                case TMNOFLAGS:
                    // no-op
                    break;
                default:
                    throw JdbdExceptions.xaInvalidFlagForStart(flags);
            }
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapStartResult)
                .thenReturn(this);
    }


    @Override
    public final Mono<XaDatabaseSession> end(final Xid xid, final int flags) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA END");
        try {
            appendXid(builder, xid);
            switch (flags) {
                case TMSUCCESS:
                case TMFAIL:
                    //no-op
                    break;
                case TMSUSPEND:
                    builder.append(" SUSPEND");
                    break;
                default:
                    throw JdbdExceptions.xaInvalidFlagForEnd(flags);
            }
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .thenReturn(this);
    }

    @Override
    public final Mono<Integer> prepare(final Xid xid) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA PREPARE");
        try {
            appendXid(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapPrepareResultCode);

    }


    @Override
    public final Mono<XaDatabaseSession> commit(Xid xid, final boolean onePhase) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA COMMIT");
        try {
            appendXid(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        if (onePhase) {
            builder.append(" ONE PHASE");
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapCommitResult)
                .thenReturn(this);
    }

    @Override
    public final Mono<XaDatabaseSession> rollback(final Xid xid) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA ROLLBACK");
        try {
            appendXid(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapRollbackResult)
                .thenReturn(this);
    }

    @Override
    public final Mono<XaDatabaseSession> forget(final Xid xid) {
        // mysql doesn't support this
        return Mono.just(this);
    }

    @Override
    public final Flux<Xid> recover(final int flags) {
        final Flux<Xid> flux;
        if (flags != TMNOFLAGS && ((flags & TMSTARTRSCAN) | (flags & TMENDRSCAN)) == 0) {
            flux = Flux.error(MySQLExceptions.xaInvalidFlagForRecover(flags));
        } else if ((flags & TMSTARTRSCAN) == 0) {
            flux = Flux.empty();
        } else {
            flux = this.protocol.query(Stmts.stmt("XA RECOVER"))
                    .map(this::mapRecoverResult);
        }
        return flux;
    }


    private void appendXid(final StringBuilder cmdBuilder, final Xid xid) throws JdbdXaException {
        Objects.requireNonNull(xid, "xid");

        final String gtrid = xid.getGtrid();
        if (MySQLStrings.hasText(gtrid)) {
            final byte[] bytes;
            bytes = gtrid.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 64) {
                throw MySQLExceptions.xaGtridBeyond64Bytes();
            }
            cmdBuilder.append(" 0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        } else {
            throw MySQLExceptions.xaGtridNoText();
        }

        final String bqual = xid.getBqual();
        if (bqual != null) {
            final byte[] bytes;
            bytes = bqual.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 64) {
                throw MySQLExceptions.xaBqualBeyond64Bytes();
            }
            cmdBuilder.append(",0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        }
        cmdBuilder.append(",")
                .append(Integer.toUnsignedString(xid.getFormatId()));

    }

    /**
     * @see #start(Xid, int)
     */
    private ResultStates mapStartResult(final ResultStates states) {
        if (this.protocol.startedTransaction(states)) {
            return states;
        }
        throw new MySQLJdbdException("XA START failure,session not in XA transaction.");
    }

    /**
     * @see #prepare(Xid)
     */
    private int mapPrepareResultCode(ResultStates states) {
        return this.protocol.isReadOnlyTransaction(states) ? XA_RDONLY : XA_OK;
    }

    /**
     * @see #commit(Xid, boolean)
     */
    private ResultStates mapCommitResult(final ResultStates states) {
        if (this.protocol.startedTransaction(states)) {
            throw new MySQLJdbdException("XA COMMIT failure,session still in transaction.");
        }
        return states;
    }

    /**
     * @see #commit(Xid, boolean)
     */
    private ResultStates mapRollbackResult(final ResultStates states) {
        if (this.protocol.startedTransaction(states)) {
            throw new MySQLJdbdException("XA ROLLBACK failure,session still in transaction.");
        }
        return states;
    }

    /**
     * @see #recover(int)
     */
    private Xid mapRecoverResult(final ResultRow row) {
        final int gtridLength, bqualLength;

        gtridLength = row.getNonNull("gtrid_length", Integer.class);
        bqualLength = row.getNonNull("bqual_length", Integer.class);

        final byte[] dataBytes;
        dataBytes = row.getNonNull("data", String.class).getBytes(StandardCharsets.UTF_8);
        if (dataBytes.length != (gtridLength + bqualLength)) {
            String m;
            m = String.format("XA Recover error,data length[%s] isn't the sum of between gtrid_length[%s] and bqual_length[%s]."
                    , dataBytes.length, gtridLength, bqualLength);
            throw new MySQLJdbdException(m);
        }

        final String gtrid, bqual;
        gtrid = new String(dataBytes, 0, gtridLength, StandardCharsets.UTF_8);
        if (bqualLength == 0) {
            bqual = null;
        } else {
            bqual = new String(dataBytes, gtridLength, bqualLength, StandardCharsets.UTF_8);
        }
        return XidImpl.create(gtrid, bqual, row.getNonNull("formatID", Integer.class));
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
