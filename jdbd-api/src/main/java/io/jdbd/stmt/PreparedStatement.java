package io.jdbd.stmt;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.result.*;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.PreparedStatement}
 * </p>
 * <p>
 * You must invoke one of below methods,or {@link io.jdbd.DatabaseSession} of this {@link PreparedStatement}
 * can't execute any new {@link Statement},because this session will wait(maybe in task queue)
 * for you invoke one of below methods.
 * <ul>
 *     <li>{@link #executeBatch()}</li>
 *     <li>{@link #executeUpdate()}</li>
 *     <li>{@link #executeQuery()}</li>
 *     <li>{@link #executeQuery(Consumer)}</li>
 *     <li>{@link #executeBatchAsMulti()}</li>
 *     <li>{@link #executeBatchMulti()}</li>
 * </ul>
 * </p>
 */
public interface PreparedStatement extends BindableSingleStatement, BindableMultiResultStatement {


    @Override
    boolean supportPublisher();

    @Override
    boolean supportOutParameter();


    List<? extends io.jdbd.meta.SQLType> getParameterMeta();

    ResultRowMeta getResultRowMeta() throws JdbdSQLException;

    /**
     * {@inheritDoc }
     */
    @Override
    void bind(int indexBasedZero, @Nullable Object nullable);

    /**
     * {@inheritDoc }
     */
    @Override
    void addBatch() throws JdbdException;

    @Override
    Publisher<ResultStates> executeUpdate();

    /**
     * @see #executeQuery(Consumer)
     */
    @Override
    Publisher<ResultRow> executeQuery();

    @Override
    Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

    @Override
    Publisher<ResultStates> executeBatch();

    @Override
    MultiResult executeBatchAsMulti();

    SafePublisher executeBatchAsFlux();


}
