package io.jdbd;

import io.jdbd.meta.SQLType;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;

public interface ResultRowMeta {

    /**
     * Returns the number of columns
     *
     * @return the number of columns
     */
    int getColumnCount();

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    JDBCType getJdbdType(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    JDBCType getJdbdType(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isPhysicalColumn(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isPhysicalColumn(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    SQLType getSQLType(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    SQLType getSQLType(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    NullMode getNullMode(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    NullMode getNullMode(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isSigned(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isSigned(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isAutoIncrement(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isAutoIncrement(String columnLabel) throws ReactiveSQLException;

    /**
     * Indicates whether a column's case matters.
     *
     * @param index base 0,the first column is 0, the second is 1, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isCaseSensitive(int index) throws ReactiveSQLException;

    /**
     * Indicates whether a column's case matters.
     *
     * @param columnLabel column alias.
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isCaseSensitive(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getCatalogName(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getCatalogName(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getSchemaName(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getSchemaName(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getTableName(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getTableName(String columnLabel) throws ReactiveSQLException;

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays. The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return the suggested column title              .
     * @throws ReactiveSQLException if a database access error occurs
     */
    String getColumnLabel(int index) throws ReactiveSQLException;

    /**
     * Get the designated column's name.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return column name
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    String getColumnName(int index) throws ReactiveSQLException;

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isReadOnly(int index) throws ReactiveSQLException;

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param columnLabel column alias.
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isReadOnly(String columnLabel) throws ReactiveSQLException;

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isWritable(int index) throws ReactiveSQLException;

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param columnLabel column alias.
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isWritable(String columnLabel) throws ReactiveSQLException;


    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>io.jdbd.ResultRow.getObject</code> may return a subclass of the
     * class returned by this method.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return the class in the Java programming
     * language that would be used by the method
     * <code>io.jdbd.ResultRow.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @throws ReactiveSQLException if a database access error occurs
     */
    Class<?> getColumnClass(int index) throws ReactiveSQLException;

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>io.jdbd.ResultRow.getObject</code> may return a subclass of the
     * class returned by this method.
     *
     * @return the class in the Java programming
     * language that would be used by the method
     * <code>io.jdbd.ResultRow.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @throws ReactiveSQLException if a database access error occurs
     */
    Class<?> getColumnClass(String columnLabel) throws ReactiveSQLException;

    /**
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return precision
     * @throws ReactiveSQLException if a database access error occurs
     */
    long getPrecision(int index) throws ReactiveSQLException;

    /**
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param columnLabel column alias.
     * @return precision
     * @throws ReactiveSQLException if a database access error occurs
     */
    long getPrecision(String columnLabel) throws ReactiveSQLException;

    /**
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param index base 0,the first column is 0, the second is 1, ..
     * @return scale ,-1 or scale
     * @throws ReactiveSQLException if a database access error occurs
     */
    int getScale(int index) throws ReactiveSQLException;

    /**
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param columnLabel column alias.
     * @return scale ,-1 or scale
     * @throws ReactiveSQLException if a database access error occurs
     */
    int getScale(String columnLabel) throws ReactiveSQLException;


    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isPrimaryKey(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isPrimaryKey(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isUniqueKey(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isUniqueKey(String columnLabel) throws ReactiveSQLException;

    /**
     * @param index base 0,the first column is 0, the second is 1, ...
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isMultipleKey(int index) throws ReactiveSQLException;

    /**
     * @param columnLabel column alias.
     * @throws ReactiveSQLException if a database access error occurs
     */
    boolean isMultipleKey(String columnLabel) throws ReactiveSQLException;


}
