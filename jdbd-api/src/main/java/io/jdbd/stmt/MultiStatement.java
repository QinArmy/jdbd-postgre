package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;

public interface MultiStatement extends BindableMultiResultStatement {


    @Override
    boolean supportLongData();

    @Override
    boolean supportOutParameter();

    void addStmt(String sql);

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
    void bind(int index, @Nullable Object nullable);


    @Override
    MultiResult executeBatchAsMulti();

    @Override
    Publisher<Result> executeBatchAsFlux();
}
