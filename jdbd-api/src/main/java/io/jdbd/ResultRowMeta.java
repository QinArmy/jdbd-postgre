package io.jdbd;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.SQLType;

import java.sql.JDBCType;
import java.util.List;

/**
 * @see ResultRow
 */
public interface ResultRowMeta {

    /**
     * Returns the number of columns
     *
     * @return the number of columns
     */
    int getColumnCount();

    /**
     * @return a unmodifiable list
     */
    List<String> getColumnAliasList();

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays. The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return the suggested column title              .
     * @throws JdbdSQLException if a database access error occurs
     */
    String getColumnLabel(int indexBaseZero) throws JdbdSQLException;

    /**
     * @param columnLabel column alias
     * @return index base 0,the first column is 0, the second is 1, ..
     * @throws JdbdSQLException if a database access error occurs
     */
    int getColumnIndex(String columnLabel) throws JdbdSQLException;

    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    JDBCType getJdbdType(int indexBaseZero) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isPhysicalColumn(int indexBaseZero) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    SQLType getSQLType(int indexBaseZero) throws JdbdSQLException;

    SQLType getSQLType(String columnAlias) throws JdbdSQLException;

    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    NullMode getNullMode(int indexBaseZero) throws JdbdSQLException;

    /**
     * @see #getNullMode(int)
     */
    NullMode getNullMode(String columnAlias) throws JdbdSQLException;

    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isUnsigned(int indexBaseZero) throws JdbdSQLException;

    /**
     * @see #isUnsigned(int)
     */
    boolean isUnsigned(String columnAlias) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException;

    /**
     * @see #isAutoIncrement(int)
     */
    boolean isAutoIncrement(String columnAlias) throws JdbdSQLException;

    /**
     * Indicates whether a column's case matters.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isCaseSensitive(int indexBaseZero) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    @Nullable
    String getCatalogName(int indexBaseZero) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    @Nullable
    String getSchemaName(int indexBaseZero) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    @Nullable
    String getTableName(int indexBaseZero) throws JdbdSQLException;


    /**
     * Get the designated column's name.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return column name
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    @Nullable
    String getColumnName(int indexBaseZero) throws JdbdSQLException;

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isReadOnly(int indexBaseZero) throws JdbdSQLException;


    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isWritable(int indexBaseZero) throws JdbdSQLException;


    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>io.jdbd.ResultRow.getObject</code> may return a subclass of the
     * class returned by this method.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return the class in the Java programming
     * language that would be used by the method
     * <code>io.jdbd.ResultRow.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    Class<?> getColumnClass(int indexBaseZero) throws JdbdSQLException;


    /**
     * get precision of column.
     * <p>
     * follow below principle:
     * <ul>
     *     <li>decimal type: max precision</li>
     *     <li>integer type: -1</li>
     *     <li>float type: - 1</li>
     *     <li>boolean type: -1</li>
     *     <li>character type: maximum character length</li>
     *     <li>datetime type: maximum allowed precision of the fractional seconds component</li>
     *     <li>binary type: maximum length of byte</li>
     *     <li>ROWID type: maximum length of byte</li>
     *     <li>other : -1</li>
     * </ul>
     * </p>
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return precision
     * @throws JdbdSQLException if a database access error occurs
     *                              @see #getColumnIndex(String)
     */
    long getPrecision(int indexBaseZero) throws JdbdSQLException;

    /**
     * @see #getPrecision(int)
     */
    long getPrecision(String columnAlias) throws JdbdSQLException;

    /**
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ..
     * @return scale ,-1 or scale
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    int getScale(int indexBaseZero) throws JdbdSQLException;

    int getScale(String columnAlias) throws JdbdSQLException;


    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isPrimaryKey(int indexBaseZero) throws JdbdSQLException;

    /**
     * @see #isPrimaryKey(int)
     */
    boolean isPrimaryKey(String columnAlias) throws JdbdSQLException;

    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isUniqueKey(int indexBaseZero) throws JdbdSQLException;

    boolean isUniqueKey(String columnAlias) throws JdbdSQLException;

    /**
     * @param indexBaseZero base 0,the first column is 0, the second is 1, ...
     * @throws JdbdSQLException if a database access error occurs
     * @see #getColumnIndex(String)
     */
    boolean isMultipleKey(int indexBaseZero) throws JdbdSQLException;

    boolean isMultipleKey(String columnAlias) throws JdbdSQLException;

}
