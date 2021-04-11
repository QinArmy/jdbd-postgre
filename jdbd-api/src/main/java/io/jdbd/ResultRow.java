package io.jdbd;

import io.jdbd.lang.Nullable;
import io.jdbd.result.ResultRowMeta;

import java.util.List;
import java.util.Set;

/**
 * @see MultiResults
 */
public interface ResultRow {

    ResultRowMeta getRowMeta();

    @Nullable
    Object get(int indexBaseZero) throws JdbdSQLException;


    @Nullable
    <T> T get(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException, UnsupportedConvertingException;


    @Nullable
    Object get(String columnAlias) throws JdbdSQLException;


    @Nullable
    <T> T get(String alias, Class<T> columnClass) throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @return a unmodifiable set.
     */
    <T> Set<T> getSet(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @see #getSet(int, Class)
     */
    <T> Set<T> getSet(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @return a unmodifiable list.
     */
    <T> List<T> getList(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @see #getSet(int, Class)
     */
    <T> List<T> getList(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    Object getNonNull(int indexBaseZero) throws JdbdSQLException, NullPointerException;

    <T> T getNonNull(int indexBaseZero, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException;

    Object getNonNull(String columnAlias) throws JdbdSQLException, NullPointerException;

    <T> T getNonNull(String columnAlias, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException;

}
