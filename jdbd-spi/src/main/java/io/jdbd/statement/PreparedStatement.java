package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;

import java.sql.JDBCType;
import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.PreparedStatement}
 * </p>
 * <p>
 * You must invoke one of below methods,or {@link DatabaseSession} of this {@link PreparedStatement}
 * can't execute any new {@link Statement},because this session will wait(maybe in task queue)
 * for you invoke one of below methods.
 * <ul>
 *     <li>{@link #executeUpdate()}</li>
 *     <li>{@link #executeQuery()}</li>
 *     <li>{@link #executeBatchUpdate()}</li>
 *     <li>{@link #executeQuery(Consumer)}</li>
 *     <li>{@link #executeBatchAsMulti()}</li>
 *     <li>{@link #executeBatchAsFlux()}</li>
 *     <li>{@link #abandonBind()}</li>
 * </ul>
 * </p>
 * <p>
 *     NOTE: {@link PreparedStatement} is auto close after you invoke executeXxx() method,or binding occur error,so
 *     {@link PreparedStatement} have no close() method.
 * </p>
 */
public interface PreparedStatement extends ServerPrepareStatement {


    List< ? extends DataType> getParamTypeList();


    @Nullable
    Warning getWaring();

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bind(int indexBasedZero,@Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bind(int indexBasedZero, JDBCType jdbcType,@Nullable  Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bind(int indexBasedZero, DataType dataType,@Nullable  Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bind(int indexBasedZero, String dataTypeName,@Nullable  Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement addBatch() throws JdbdException;


}
