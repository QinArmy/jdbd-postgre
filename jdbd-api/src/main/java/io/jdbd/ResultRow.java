package io.jdbd;

import io.jdbd.lang.Nullable;

/**
 * @see MultiResults
 */
public interface ResultRow {

    ResultRowMeta getRowMeta();

    @Nullable
    Object getObject(int indexBaseZero) throws JdbdSQLException;


    @Nullable
    <T> T getObject(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException;


    @Nullable
    Object getObject(String alias) throws JdbdSQLException;


    @Nullable
    <T> T getObject(String alias, Class<T> columnClass) throws JdbdSQLException;


    Object getRequiredObject(int indexBaseZero) throws JdbdSQLException;

    <T> T getRequiredObject(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException;

    Object getRequiredObject(String alias) throws JdbdSQLException;

    <T> T getRequiredObject(String alias, Class<T> columnClass) throws JdbdSQLException;

}
