package io.jdbd.session;

import io.jdbd.env.JdbdEnvironment;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.*;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


public interface DatabaseSession  extends StaticStatementSpec ,Closeable{


    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    Publisher<ResultStates> executeUpdate(String sql);

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
     <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) ;

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
     <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer);

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    Publisher<ResultStates> executeBatchUpdate(List<String> sqlGroup);

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    MultiResult executeBatchAsMulti(List<String> sqlGroup);

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    OrderedFlux executeBatchAsFlux(List<String> sqlGroup);

    /**
     * <p>
     *     Execute sql without any statement option. So dont' create {@link StaticStatement}.
     * </p>
     */
    @Override
    OrderedFlux executeAsFlux(String multiStmt);

    DatabaseMetaData getDatabaseMetaData();


    Publisher<TransactionOption> getTransactionOption();


    StaticStatement statement();

    /**
     * <p>
     * This method is similarly to {@code java.sql.Connection#prepareStatement(String)}
     * except that is async emit a {@link PreparedStatement}.
     * </p>
     *
     * @return A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by
     * emitting an element, or with an error. Like {@code reactor.core.publisher.Mono}
     * @see #oneStep(String)
     */
    Publisher<PreparedStatement> prepare(String sql);

    /**
     * @see #prepare(String)
     */
    OneStepPrepareStatement oneStep(String sql);

    BindStatement bindStatement(String sql);

    MultiStatement multiStatement();

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean supportSavePoints();

    boolean supportStmtVar();

    boolean supportMultiStatement();


    Publisher<SavePoint> setSavePoint();


    Publisher<SavePoint> setSavePoint(String name);


    Publisher<Void> releaseSavePoint(SavePoint savepoint);


    Publisher<Void> rollbackToSavePoint(SavePoint savepoint);


    /**
     * @see Connection#isClosed()
     */
    boolean isClosed();

    ServerVersion serverVersion();


    boolean isSameFactory(DatabaseSession session);

    boolean isBelongTo(DatabaseSessionFactory factory);

    JdbdEnvironment environment();


}
