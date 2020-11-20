package io.jdbd;

import java.sql.JDBCType;

/**
 * <p>
 *     This interface is reactive version of {@link java.sql.PreparedStatement}
 * </p>
 */
public interface PreparedStatement extends GenericStatement{

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param index        the first parameter is 1, the second is 2, ...
     * @param jdbcType     nonNullValue mapping {@link JDBCType}
     * @param nonNullValue non null the parameter value
     */
    PreparedStatement bind(int index, JDBCType jdbcType, Object nonNullValue) throws ReactiveSQLException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param index                the first parameter is 1, the second is 2, ...
     * @param nonNullValue         non null the parameter value
     * @param upperCaseSQLTypeName nonNullValue mapping sql data type name(must upper case).
     */
    PreparedStatement bind(int index, String upperCaseSQLTypeName, Object nonNullValue) throws ReactiveSQLException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param index the first parameter is 1, the second is 2, ...
     */
    PreparedStatement bindNull(int index, JDBCType jdbcType) throws ReactiveSQLException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param index                the first parameter is 1, the second is 2, ...
     * @param upperCaseSQLTypeName upper case sql data type name,eg: BIGINT
     */
    PreparedStatement bindNull(int index, String upperCaseSQLTypeName) throws ReactiveSQLException;

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code>
     * object's batch of commands.
     *
     * @throws ReactiveSQLException if a database access error occurs or
     *                      this method is called on a closed <code>PreparedStatement</code>
     */
    PreparedStatement addBatch() throws ReactiveSQLException;



}
