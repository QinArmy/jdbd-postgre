package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

/**
 * <p>
 * This interface is base interface of following:
 *     <ul>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link OneStepPrepareStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface ServerPrepareStatement extends BindSingleStatement {


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bindStmtVar(String name, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement addBatch() throws JdbdException;


}
