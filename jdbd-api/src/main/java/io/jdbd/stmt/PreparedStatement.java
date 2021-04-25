package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import org.reactivestreams.Publisher;

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
 *     <li>{@link #executeAsMulti()}</li>
 *     <li>{@link #executeBatchMulti()}</li>
 * </ul>
 * </p>
 */
public interface PreparedStatement extends BindableSingleStatement, BindableMultiResultStatement {


    @Override
    boolean supportLongData();

    @Override
    boolean supportOutParameter();

    /**
     * {@inheritDoc }
     */
    @Override
    void bind(int indexBasedZero, @Nullable Object nullable);

    /**
     * {@inheritDoc }
     */
    @Override
    void addBatch();

    @Override
    Publisher<ResultState> executeBatch();

    @Override
    Publisher<ResultState> executeUpdate();

    /**
     * @see #executeQuery(Consumer)
     */
    @Override
    Publisher<ResultRow> executeQuery();

    @Override
    Publisher<ResultRow> executeQuery(Consumer<ResultState> statesConsumer);

    @Override
    MultiResult executeAsMulti();

    Publisher<SingleResult> executeAsFlux();


    /**
     * <p>
     * Only below methods support this method:
     *     <ul>
     *         <li>{@link #executeQuery()}</li>
     *         <li>{@link #executeQuery(Consumer)}</li>
     *     </ul>
     * </p>
     * <p>
     * invoke before invoke {@link #executeQuery()} or {@link #executeQuery(Consumer)}.
     * </p>
     *
     * @param fetchSize fetch size ,positive support
     * @return true :<ul>
     * <li>fetchSize great than zero</li>
     * <li>driver implementation support fetch</li>
     * </ul>
     */
    boolean setFetchSize(int fetchSize);

}
