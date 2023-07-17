package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.result.BatchQuery;
import io.jdbd.result.ResultRow;
import io.jdbd.session.HandleMode;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.TransactionOption;
import io.jdbd.statement.BindStatement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;

class PgLocalDatabaseSession extends PgDatabaseSession implements LocalDatabaseSession {


    static PgLocalDatabaseSession create(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgLocalDatabaseSession(adjutant, protocol);
    }

    static PgLocalDatabaseSession forPoolVendor(SessionAdjutant adjutant, ClientProtocol protocol) {
        return new PgPoolLocalDatabaseSession(adjutant, protocol);
    }


    private PgLocalDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        super(adjutant, protocol);
    }


    @Override
    public final Mono<LocalDatabaseSession> startTransaction(TransactionOption option) {
        return null;
    }


    @Override
    public BindStatement bindStatement(String sql, boolean forceServerPrepared) throws JdbdException {
        return null;
    }

    @Override
    public boolean supportStmtVar() throws JdbdException {
        return false;
    }

    @Override
    public boolean supportOutParameter() throws JdbdException {
        return false;
    }

    @Override
    public Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, HandleMode mode) {
        return null;
    }

    @Override
    public boolean inTransaction() throws JdbdException {
        return false;
    }

    @Override
    public Publisher<ResultRow> executeQuery(String sql) {
        return null;
    }

    @Override
    public BatchQuery executeBatchQuery(List<String> sqlGroup) {
        return null;
    }

    @Override
    public final Mono<LocalDatabaseSession> commit() {
        return null;
    }

    @Override
    public final Mono<LocalDatabaseSession> rollback() {
        return null;
    }


    private static final class PgPoolLocalDatabaseSession extends PgLocalDatabaseSession
            implements PoolLocalDatabaseSession {

        private PgPoolLocalDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
            super(adjutant, protocol);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> ping(final int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }

    }


}
