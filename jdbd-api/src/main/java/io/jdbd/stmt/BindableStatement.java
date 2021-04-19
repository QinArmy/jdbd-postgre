package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;

public interface BindableStatement extends BindableSingleStatement, BindableMultiResultStatement {


    boolean supportLongData();


    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullable       nullable null the parameter value
     */
    void bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable);

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param sqlType        nonNullValue mapping sql data type name(must upper case).
     */
    void bind(int indexBasedZero, io.jdbd.meta.SQLType sqlType, @Nullable Object nullable);


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

    MultiResult executeBatchMulti();

}
