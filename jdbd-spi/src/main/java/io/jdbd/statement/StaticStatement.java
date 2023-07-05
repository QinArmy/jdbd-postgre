package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.Statement}
 * </p>
 */
public interface StaticStatement extends Statement, StaticStatementSpec {


    @Override
    StaticStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    @Override
    StaticStatement bindStmtVar(String name, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    @Override
    StaticStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;
}
