package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;

public interface MultiStatement extends BindMultiResultStatement,ParameterStatement {

    /**
     * @param sql must have text.
     * @throws IllegalArgumentException when sql has no text.
     * @throws JdbdException            when reuse this instance after invoke below method:
     *                                  <ul>
     *                                      <li>{@link #executeBatchUpdate()}</li>
     *                                      <li>{@link #executeBatchAsMulti()}</li>
     *                                      <li>{@link #executeBatchAsFlux()}</li>
     *                                  </ul>
     */
    MultiStatement addStatement(String sql) throws JdbdException;

    @Override
    MultiStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullable       nullable null the parameter value
     */
    @Override
    MultiStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param dataType        nonNullValue mapping sql data type name(must upper case).
     */
    @Override
    MultiStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    @Override
    MultiStatement bind(int indexBasedZero, String dataTypeName, @Nullable Object nullable) throws JdbdException;

    @Override
    Publisher<ResultStates> executeBatchUpdate();

    @Override
    MultiResult executeBatchAsMulti();

    @Override
    OrderedFlux executeBatchAsFlux();


}
