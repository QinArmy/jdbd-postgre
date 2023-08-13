package io.jdbd.postgre.session;

import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;


/**
 * <p>
 * This class is a implementation of {@link LocalDatabaseSession} with postgre client protocol.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-start-transaction.html">START TRANSACTION</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">COMMIT</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-rollback.html">ROLLBACK</a>
 * @since 1.0
 */
class PgLocalDatabaseSession extends PgDatabaseSession<LocalDatabaseSession> implements LocalDatabaseSession {


    static LocalDatabaseSession create(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgLocalDatabaseSession(factory, protocol);
    }

    static PoolLocalDatabaseSession forPoolVendor(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        return new PgPoolLocalDatabaseSession(factory, protocol);
    }

    /**
     * <p>
     * private constructor.
     * </p>
     */
    private PgLocalDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
        super(factory, protocol);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-start-transaction.html">START TRANSACTION</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> startTransaction(TransactionOption option) {
        return this.protocol.startTransaction(option, HandleMode.ERROR_IF_EXISTS)
                .thenReturn(this);
    }

    /**
     * <p>
     * PostgreSQL support following option :
     *     <ul>
     *         <li>{@link Option#DEFERRABLE}</li>
     *     </ul>
     * </p>
     *
     * @see <a href="https://www.postgresql.org/docs/current/sql-start-transaction.html">START TRANSACTION</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> startTransaction(TransactionOption option, HandleMode mode) {
        return this.protocol.startTransaction(option, mode)
                .thenReturn(this);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-set-transaction.html">SET TRANSACTION</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-ISOLATION">transaction_isolation</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-READ-ONLY">transaction_read_only</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-DEFERRABLE">transaction_deferrable</a>
     */
    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        return this.protocol.transactionStatus();
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">COMMIT</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> commit() {
        return this.protocol.commit(Collections.emptyMap())
                .thenReturn(this);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">COMMIT</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> commit(Map<Option<?>, ?> optionMap) {
        return this.protocol.commit(optionMap)
                .thenReturn(this);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-rollback.html">ROLLBACK</a>
     */
    @Override
    public final Mono<LocalDatabaseSession> rollback() {
        return this.protocol.rollback(Collections.emptyMap())
                .thenReturn(this);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-rollback.html">ROLLBACK</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> rollback(Map<Option<?>, ?> optionMap) {
        return this.protocol.rollback(optionMap)
                .thenReturn(this);
    }


    private static final class PgPoolLocalDatabaseSession extends PgLocalDatabaseSession
            implements PoolLocalDatabaseSession {


        private PgPoolLocalDatabaseSession(PgDatabaseSessionFactory factory, PgProtocol protocol) {
            super(factory, protocol);
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

        @Override
        public Publisher<PoolLocalDatabaseSession> reconnect() {
            return this.protocol.reconnect()
                    .thenReturn(this);
        }


    }//PgPoolLocalDatabaseSession


}
