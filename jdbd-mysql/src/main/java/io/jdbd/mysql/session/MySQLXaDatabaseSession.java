package io.jdbd.mysql.session;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLBuffers;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.pool.PoolXaDatabaseSession;
import io.jdbd.session.XaDatabaseSession;
import io.jdbd.session.Xid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
        final StringBuilder cmdBuilder = new StringBuilder();

        try {
            cmdBuilder.append("XA START ");
            appendXid(cmdBuilder, xid);
            switch (flags) {
                case TMJOIN:
                    cmdBuilder.append(" JOIN");
                    break;
                case TMRESUME:
                    cmdBuilder.append(" RESUME");
                    break;
                case TMNOFLAGS:
                    // no-op
                    break;
                default:

            }
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(cmdBuilder.toString()))
                .thenReturn(this);
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

    private void appendXid(final StringBuilder cmdBuilder, final Xid xid) {
        Objects.requireNonNull(xid, "xid");

        final String gtrid = xid.getGtrid();
        if (MySQLStrings.hasText(gtrid)) {
            final byte[] bytes;
            bytes = gtrid.getBytes(StandardCharsets.UTF_8);
            cmdBuilder.append(" 0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        } else {
            throw new JdbdSQLException(new SQLException("gtrid of xid must have text."));
        }

        final String bqual = xid.getBqual();
        if (bqual != null) {
            final byte[] bytes;
            bytes = bqual.getBytes(StandardCharsets.UTF_8);
            cmdBuilder.append(",0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        }
        cmdBuilder.append(",")
                .append(Integer.toUnsignedString(xid.getFormatId()));

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
