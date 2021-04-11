package io.jdbd;

import io.jdbd.lang.Nullable;
import io.jdbd.stmt.ExecutableStatement;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;

public interface BindableStatement extends ExecutableStatement {

    @Override
    BindableStatement addBatch();

    boolean supportLongData();


    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullableValue  nullable null the parameter value
     */
    BindableStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullableValue);

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullableValue  nullable the parameter value
     * @param sqlType        nonNullValue mapping sql data type name(must upper case).
     */
    BindableStatement bind(int indexBasedZero, io.jdbd.meta.SQLType sqlType, @Nullable Object nullableValue);


    @Override
    BindableStatement bind(int indexBasedZero, @Nullable Object nullableValue);

    @Override
    ExecutableStatement setFetchSize(int fetchSize);


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
    BindableStatement setExecuteTimeout(int seconds);
}
