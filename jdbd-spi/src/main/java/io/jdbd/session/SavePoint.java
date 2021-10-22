package io.jdbd.session;

import io.jdbd.JdbdSQLException;

/**
 * The representation of a savepoint, which is a point within
 * the current transaction that can be referenced from the
 * <code>Connection.rollback</code> method. When a transaction
 * is rolled back to a savepoint all changes made after that
 * savepoint are undone.
 * <p>
 * SavePoints can be either named or unnamed. Unnamed SavePoints
 * are identified by an ID generated by the underlying data source.
 * <p>
 * This interface same with {@code java.sql.Savepoint}
 * ,but throw {@link JdbdSQLException} not {@code java.sql.SQLException}.
 * </p>
 *
 * @since 1.0
 */

public interface SavePoint {

    /**
     * Retrieves the generated ID for the savepoint that this
     * <code>Savepoint</code> object represents.
     *
     * @return the numeric ID of this savepoint
     * @throws JdbdSQLException if this is a named savepoint
     * @since 1.0
     */
    int getSavePointId() throws JdbdSQLException;

    /**
     * Retrieves the name of the savepoint that this <code>Savepoint</code>
     * object represents.
     *
     * @return the name of this savepoint
     * @throws JdbdSQLException if this is an un-named savepoint
     * @since 1.0
     */
    String getSavePointName() throws JdbdSQLException;
}