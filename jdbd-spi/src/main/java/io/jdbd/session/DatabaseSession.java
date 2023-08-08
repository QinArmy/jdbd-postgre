package io.jdbd.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.statement.*;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This interface representing database session, This interface is reactive version of {@code java.sql.Connection}.
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

    /**
     * <p>
     * This method create one {@link DatabaseMetaData} instance.
     * </p>
     *
     * @throws JdbdException throw when session have closed.
     */
    DatabaseMetaData databaseMetaData() throws JdbdException;


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

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // session is instance of DatabaseSession
     *             session.bindStatement(sql,false) ;
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #bindStatement(String, boolean)
     */
    BindStatement bindStatement(String sql) throws JdbdException;

    /**
     * <p>
     * Create the statement that is the adaptor of client-prepared statement and server-prepared statement.
     * </p>
     * <p>
     * This method don't check session whether open or not.
     * </p>
     *
     * @param sql                 have text sql.
     * @param forceServerPrepared true : must use server-prepared statement.
     * @throws IllegalArgumentException throw when only sql have no text.
     * @see BindStatement#isForcePrepare()
     */
    BindStatement bindStatement(String sql, boolean forceServerPrepared);

    /**
     * <p>
     * Create one multi statement.
     * </p>
     *
     * @throws JdbdException throw when :
     *                       <ul>
     *                           <li>session have closed</li>
     *                           <li>{@link #isSupportMultiStatement()} return false</li>
     *                       </ul>
     */
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


    boolean isSameFactory(DatabaseSession session);


    /**
     * <p>
     * This method should provide the access of some key(<strong>NOTE</strong> : is key,not all) properties of url ,but {@link io.jdbd.Driver#PASSWORD},
     * see {@link io.jdbd.Driver#createSessionFactory(String, Map)}.<br/>
     * </p>
     *
     * <p>
     * The implementation of this method must provide java doc(html list) for explaining supporting {@link Option} list.
     * </p>
     *
     * <p>
     * The implementation of this method perhaps support some of following :
     *     <ul>
     *         <li>{@link Option#AUTO_COMMIT}</li>
     *         <li>{@link Option#IN_TRANSACTION}</li>
     *         <li>{@link Option#READ_ONLY},true :  representing exists transaction and is read only.</li>
     *         <li>{@link Option#CLIENT_ZONE}</li>
     *         <li>{@link Option#SERVER_ZONE}</li>
     *         <li>{@link Option#CLIENT_CHARSET}</li>
     *         <li>{@link Option#BACKSLASH_ESCAPES}</li>
     *         <li>{@link Option#BINARY_HEX_ESCAPES}</li>
     *         <li>{@link Option#AUTO_RECONNECT}</li>
     *     </ul>
     * </p>
     *
     * @return null or the value of option.
     * @throws JdbdException throw when option need session open and session have closed.
     */
    @Nullable
    @Override
    <T> T valueOf(Option<T> option) throws JdbdException;


}
