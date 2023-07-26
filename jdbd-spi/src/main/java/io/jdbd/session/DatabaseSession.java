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
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface DatabaseSession extends StaticStatementSpec, Closeable {



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

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean isSupportSavePoints() throws JdbdException;

    boolean isSupportStmtVar() throws JdbdException;

    boolean isSupportMultiStatement() throws JdbdException;

    boolean isSupportOutParameter() throws JdbdException;


    boolean isSupportStoredProcedures() throws JdbdException;


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

    ServerVersion serverVersion();


    boolean isSameFactory(DatabaseSession session);


}
