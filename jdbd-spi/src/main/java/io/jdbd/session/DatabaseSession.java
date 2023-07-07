package io.jdbd.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.*;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

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


    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    Publisher<ResultStates> executeUpdate(String sql);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    Publisher<ResultStates> executeBatchUpdate(List<String> sqlGroup);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    MultiResult executeBatchAsMulti(List<String> sqlGroup);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    OrderedFlux executeBatchAsFlux(List<String> sqlGroup);

    /**
     * <p>
     * Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    OrderedFlux executeAsFlux(String multiStmt);

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

    BindStatement bindStatement(String sql);

    /**
     * @param forcePrepare true : must use server prepare statement.
     * @see BindStatement#isForcePrepare()
     */
    BindStatement bindStatement(String sql, boolean forcePrepare) throws JdbdException;

    MultiStatement multiStatement() throws JdbdException;

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean supportSavePoints();

    boolean supportStmtVar();

    boolean supportMultiStatement();

    boolean supportOutParameter();


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
