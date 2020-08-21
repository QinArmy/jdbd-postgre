package io.jdbd;

import java.sql.JDBCType;
import java.sql.SQLException;

/**
 * <p>
 *     This interface is reactive version of {@link java.sql.PreparedStatement}
 * </p>
 */
public interface PreparedStatement extends GenericStatement{

    /**
     * <p>
     *     SQL parameter placeholder must be {@code ?}
     * </p>
     * @param index        the first parameter is 1, the second is 2, ...
     * @param jdbcType     nonNullValue mapping {@link JDBCType}
     * @param nonNullValue non null the parameter value
     */
    void bind(int index, JDBCType jdbcType, Object nonNullValue) throws SQLException;

    /**
     * <p>
     *     SQL parameter placeholder must be {@code ?}
     * </p>
     * @param index                the first parameter is 1, the second is 2, ...
     * @param nonNullValue         non null the parameter value
     * @param upperCaseSQLTypeName nonNullValue mapping sql data type name(must upper case).
     */
    void bind(int index, String upperCaseSQLTypeName, Object nonNullValue) throws SQLException;

    /**
     * <p>
     *     SQL parameter placeholder must be {@code ?}
     * </p>
     * @param index the first parameter is 1, the second is 2, ...
     */
    void bindNull(int index, JDBCType jdbcType) throws SQLException;

    /**
     * <p>
     *     SQL parameter placeholder must be {@code ?}
     * </p>
     * @param index                the first parameter is 1, the second is 2, ...
     * @param upperCaseSQLTypeName upper case sql data type name,eg: BIGINT
     */
    void bindNull(int index, String upperCaseSQLTypeName) throws SQLException;

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code>
     * object's batch of commands.
     *
     * @throws SQLException if a database access error occurs or
     *                      this method is called on a closed <code>PreparedStatement</code>
     */
    PreparedStatement addBatch() throws SQLException;



}
