package io.jdbd.stmt;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;

public interface MultiStatement extends BindableMultiResultStatement {

    /**
     * @param sql must have text.
     * @throws IllegalArgumentException when sql has no text.
     * @throws JdbdException            when reuse this instance after invoke below method:
     *                                  <ul>
     *                                      <li>{@link #executeBatch()}</li>
     *                                      <li>{@link #executeBatchAsMulti()}</li>
     *                                      <li>{@link #executeBatchAsFlux()}</li>
     *                                  </ul>
     */
    void addStatement(String sql) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullable       nullable null the parameter value
     */
    void bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param sqlType        nonNullValue mapping sql data type name(must upper case).
     */
    void bind(int indexBasedZero, io.jdbd.meta.SQLType sqlType, @Nullable Object nullable) throws JdbdException;


    @Override
    void bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;


    @Override
    Publisher<ResultStates> executeBatch();

    @Override
    MultiResult executeBatchAsMulti();

    @Override
    Publisher<Result> executeBatchAsFlux();


}
