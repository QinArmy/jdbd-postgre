package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

public interface BindStatement extends BindSingleStatement {

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bindStmtVar(String name, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement addBatch() throws JdbdException;


}
