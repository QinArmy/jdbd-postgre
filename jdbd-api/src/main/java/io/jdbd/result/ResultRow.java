package io.jdbd.result;

import io.jdbd.JdbdSQLException;
import io.jdbd.lang.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @see MultiResult
 */
public interface ResultRow extends Result {

    /**
     * <p>
     * This method invoke always {@link ResultRowMeta#getResultIndex()},The implementation of this interface
     * never store result index value.
     * </p>
     */
    @Override
    int getResultIndex();

    ResultRowMeta getRowMeta();

    @Nullable
    Object get(int indexBaseZero) throws JdbdSQLException;


    /**
     * <p>
     * This method is equivalent to below:
     * <pre>
     *        final int columnIndex  = this.getRowMeta().getColumnIndex(columnLabel)
     *        return get(columnIndex);
     *     </pre>
     * </p>
     *
     * @see #get(int)
     */
    @Nullable
    Object get(String columnLabel);


    @Nullable
    <T> T get(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException, UnsupportedConvertingException;


    /**
     * <p>
     * This method is equivalent to below:
     * <pre>
     *        final int columnIndex  = this.getRowMeta().getColumnIndex(columnLabel)
     *        return get(columnIndex,columnClass);
     *     </pre>
     * </p>
     *
     * @see #get(int, Class)
     */
    @Nullable
    <T> T get(String columnLabel, Class<T> columnClass);

    Object getNonNull(int indexBaseZero) throws NullPointerException;

    <T> T getNonNull(int indexBaseZero, Class<T> columnClass) throws NullPointerException;

    Object getNonNull(String columnLabel);

    <T> T getNonNull(String columnLabel, Class<T> columnClass) throws NullPointerException;

    /**
     * @return a unmodifiable list.
     */
    <T> List<T> getList(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @see #getSet(int, Class)
     */
    <T> List<T> getList(String columnLabel, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @return a unmodifiable set.
     */
    <T> Set<T> getSet(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    /**
     * @see #getSet(int, Class)
     */
    <T> Set<T> getSet(String columnLabel, Class<T> elementClass);


    <K, V> Map<K, V> getMap(int indexBaseZero, Class<K> keyClass, Class<V> valueClass)
            throws JdbdSQLException, UnsupportedConvertingException;

    <K, V> Map<K, V> getMap(String columnLabel, Class<K> keyClass, Class<V> valueClass);


}
