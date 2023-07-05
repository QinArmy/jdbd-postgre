package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

public interface OneStepPrepareStatement extends ServerPrepareStatement {

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bindStmtVar(String name, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement addBatch() throws JdbdException;


}
