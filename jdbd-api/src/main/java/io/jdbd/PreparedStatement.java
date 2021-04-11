package io.jdbd;

import io.jdbd.lang.Nullable;
import io.jdbd.stmt.ExecutableStatement;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * <p>
 * This interface is reactive version of {@link java.sql.PreparedStatement}
 * </p>
 */
public interface PreparedStatement extends ExecutableStatement {


    @Override
    ExecutableStatement addBatch();

    @Override
    Publisher<Long> executeBatch();

    @Override
    Publisher<ResultStates> executeBatchAsStates();

    @Override
    Publisher<Long> executeUpdate();

    @Override
    Publisher<ResultStates> executeUpdateAsStates();

    @Override
    Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

    @Override
    PreparedStatement bind(int index, @Nullable Object nullableValue);

    @Override
    PreparedStatement setFetchSize(int fetchSize);
}
