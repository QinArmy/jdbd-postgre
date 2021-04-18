package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.PreparedStatement}
 * </p>
 * <p>
 * You must invoke one of below methods,or {@link io.jdbd.DatabaseSession} of this {@link PreparedStatement}
 * can't execute any new {@link Statement}.
 * <ul>
 *     <li>{@link #executeBatch()}</li>
 *     <li>{@link #executeUpdate()}</li>
 *     <li>{@link #executeQuery()}</li>
 *     <li>{@link #executeQuery(Consumer)}</li>
 *     <li>{@link #executeMulti()}</li>
 *     <li>{@link #executeBatchMulti()}</li>
 * </ul>
 * </p>
 */
public interface PreparedStatement extends BindableSingleStatement, BindableMultiResultStatement {

    @Override
    void bind(int indexBasedZero, @Nullable Object nullable);

    @Override
    void addBatch();

    @Override
    Publisher<ResultStates> executeBatch();

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
    MultiResult executeMulti();

    Publisher<MultiResult> executeBatchMulti();

    void setFetchSize(int fetchSize);

}
