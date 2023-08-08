package io.jdbd.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.statement.*;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface representing database session, This interface is reactive version of {@code   java.sql.Connection}.
 * </p>
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link LocalDatabaseSession}</li>
 *         <li>{@link RmDatabaseSession}</li>
 *         <li>{@link io.jdbd.pool.PoolLocalDatabaseSession}</li>
 *         <li>{@link io.jdbd.pool.PoolRmDatabaseSession}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface DatabaseSession extends StaticStatementSpec, SessionMetaSpec, Closeable {

    /**
     * @see DatabaseSessionFactory#name()
     */
    String factoryName();

    /**
     * <p>
     * Session identifier,probably is following :
     *     <ul>
     *         <li>process id</li>
     *         <li>thread id</li>
     *         <li>other identifier</li>
     *     </ul>
     *     <strong>NOTE</strong>: identifier will probably be updated if reconnect.
     * </p>
     *
     * @throws JdbdException throw when session have closed.
     */
    long identifier() throws JdbdException;


    DatabaseMetaData databaseMetaData();


    Publisher<TransactionStatus> transactionStatus();


    StaticStatement statement();

    /**
     * <p>
     * This method is similarly to {@code java.sql.Connection#prepareStatement(String)}
     * except that is async emit a {@link PreparedStatement}.
     * </p>
     *
     * @return A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by
     * emitting an element, or with an error. Like {@code reactor.core.publisher.Mono}
     */
    Publisher<PreparedStatement> prepare(String sql);

    BindStatement bindStatement(String sql) throws JdbdException;

    /**
     * <p>
     * Create the statement that is the adaptor of client-prepared statement and server-prepared statement.
     * </p>
     *
     * @param forceServerPrepared true : must use server-prepared statement.
     * @see BindStatement#isForcePrepare()
     */
    BindStatement bindStatement(String sql, boolean forceServerPrepared) throws JdbdException;

    MultiStatement multiStatement() throws JdbdException;



    Publisher<SavePoint> setSavePoint();


    Publisher<SavePoint> setSavePoint(String name);


    /**
     * @return the {@link Publisher} that completes successfully by
     * emitting an element(<strong>this</strong>), or with an error. Like {@code  reactor.core.publisher.Mono}
     */
    Publisher<? extends DatabaseSession> releaseSavePoint(SavePoint savepoint);


    /**
     * @return the {@link Publisher} that completes successfully by
     * emitting an element(<strong>this</strong>), or with an error. Like {@code  reactor.core.publisher.Mono}
     */
    Publisher<? extends DatabaseSession> rollbackToSavePoint(SavePoint savepoint);


    boolean isClosed();

    /**
     * @throws JdbdException throw when session have closed.
     */
    ServerVersion serverVersion() throws JdbdException;


    boolean isSameFactory(DatabaseSession session);


}
