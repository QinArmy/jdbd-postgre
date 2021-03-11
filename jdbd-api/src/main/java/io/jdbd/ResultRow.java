package io.jdbd;

import io.jdbd.lang.Nullable;

/**
 * @see MultiResults
 */
public interface ResultRow {

    ResultRowMeta obtainRowMeta();

    @Nullable
    Object get(int indexBaseZero) throws JdbdSQLException;


    @Nullable
    <T> T get(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException;


    @Nullable
    Object get(String alias) throws JdbdSQLException;


    @Nullable
    <T> T get(String alias, Class<T> columnClass) throws JdbdSQLException;


    Object obtain(int indexBaseZero) throws JdbdSQLException;

    <T> T obtain(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException;

    Object obtain(String alias) throws JdbdSQLException;

    <T> T obtain(String alias, Class<T> columnClass) throws JdbdSQLException;

}
