package io.jdbd;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.List;

public interface GenericStatement {

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which must be an SQL Data Manipulation Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or
     * <code>DELETE</code>; or an SQL statement that returns nothing,
     * such as a DDL statement.
     *
     * <p>
     * Mono throw {@link SQLException}, if a database access error occurs;
     * this method is called on a closed  <code>PreparedStatement</code>
     * or the SQL statement returns a <code>ReactiveResultSet</code> object
     * </p>
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
     * or (2) 0 for SQL statements that return nothing
     * @see java.sql.PreparedStatement#executeUpdate()
     */
    Mono<Integer> executeUpdate();

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which must be an SQL Data Manipulation Language (DML) statement,
     * such as <code>INSERT</code>, <code>UPDATE</code> or
     * <code>DELETE</code>; or an SQL statement that returns nothing,
     * such as a DDL statement.
     * <p>
     * This method should be used when the returned row count may exceed
     * {@link Integer#MAX_VALUE}.
     * <p>
     * The default implementation will throw {@code UnsupportedOperationException}
     *
     * <p>
     * Mono throw {@link SQLException} if a database access error occurs;
     * this method is called on a closed  <code>PreparedStatement</code>
     * or the SQL statement returns a <code>ReactiveResultSet</code> object
     * </p>
     *
     * @return either (1) the row count for SQL Data Manipulation Language
     * (DML) statements or (2) 0 for SQL statements that return nothing
     * @see java.sql.PreparedStatement#executeLargeUpdate()
     */
    Mono<Long> executeLargeUpdate();

    /**
     * Submits a batch of commands to the database for execution and
     * if all commands execute successfully, returns an array of update counts.
     * The <code>int</code> elements of the array that is returned are ordered
     * to correspond to the commands in the batch, which are ordered
     * according to the order in which they were added to the batch.
     * The elements in the array returned by the method <code>executeBatch</code>
     * may be one of the following:
     * <OL>
     * <LI>A number greater than or equal to zero -- indicates that the
     * command was processed successfully and is an update count giving the
     * number of rows in the database that were affected by the command's
     * execution
     * <LI>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was
     * processed successfully but that the number of rows affected is
     * unknown
     * <P>
     * If one of the commands in a batch update fails to execute properly,
     * this method throws a <code>BatchUpdateException</code>, and a JDBC
     * driver may or may not continue to process the remaining commands in
     * the batch.  However, the driver's behavior must be consistent with a
     * particular DBMS, either always continuing to process commands or never
     * continuing to process commands.  If the driver continues processing
     * after a failure, the array returned by the method
     * <code>BatchUpdateException.getUpdateCounts</code>
     * will contain as many elements as there are commands in the batch, and
     * at least one of the elements will be the following:
     *
     * <LI>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed
     * to execute successfully and occurs only if a driver continues to
     * process commands after a command fails
     * </OL>
     * <P>
     * The possible implementations and return values have been modified in
     * the Java 2 SDK, Standard Edition, version 1.3 to
     * accommodate the option of continuing to process commands in a batch
     * update after a <code>BatchUpdateException</code> object has been thrown.
     *
     * <p>
     * Mono throw {@link SQLException} if a database access error occurs,
     * this method is called on a closed <code>Statement</code> or the
     * driver does not support batch statements. Throws {@link BatchUpdateException}
     * (a subclass of <code>SQLException</code>) if one of the commands sent to the
     * database fails to execute properly or attempts to return a result set.
     * </p>
     *
     * @return an {@link List} of update counts containing one element for each
     * command in the batch.  The elements of the array are ordered according
     * to the order in which commands were added to the batch.
     * @see PreparedStatement#addBatch()
     * @see Statement#addBatch(String)
     * @see java.sql.PreparedStatement#executeBatch()
     */
    Mono<List<Integer>> executeBatch();

    /**
     * Submits a batch of commands to the database for execution and
     * if all commands execute successfully, returns an array of update counts.
     * The <code>long</code> elements of the array that is returned are ordered
     * to correspond to the commands in the batch, which are ordered
     * according to the order in which they were added to the batch.
     * The elements in the array returned by the method {@code executeLargeBatch}
     * may be one of the following:
     * <OL>
     * <LI>A number greater than or equal to zero -- indicates that the
     * command was processed successfully and is an update count giving the
     * number of rows in the database that were affected by the command's
     * execution
     * <LI>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was
     * processed successfully but that the number of rows affected is
     * unknown
     * <P>
     * If one of the commands in a batch update fails to execute properly,
     * this method throws a <code>BatchUpdateException</code>, and a JDBC
     * driver may or may not continue to process the remaining commands in
     * the batch.  However, the driver's behavior must be consistent with a
     * particular DBMS, either always continuing to process commands or never
     * continuing to process commands.  If the driver continues processing
     * after a failure, the array returned by the method
     * <code>BatchUpdateException.getLargeUpdateCounts</code>
     * will contain as many elements as there are commands in the batch, and
     * at least one of the elements will be the following:
     *
     * <LI>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed
     * to execute successfully and occurs only if a driver continues to
     * process commands after a command fails
     * </OL>
     * <p>
     * This method should be used when the returned row count may exceed
     * {@link Integer#MAX_VALUE}.
     * <p>
     * The default implementation will throw {@code UnsupportedOperationException}
     * <p>
     * Mono throw {@link SQLException} if a database access error occurs,
     * this method is called on a closed <code>Statement</code> or the
     * driver does not support batch statements. Throws {@link BatchUpdateException}
     * (a subclass of <code>SQLException</code>) if one of the commands sent to the
     * database fails to execute properly or attempts to return a result set.
     * </p>
     *
     * @return an array of update counts containing one element for each
     * command in the batch.  The elements of the array are ordered according
     * to the order in which commands were added to the batch.
     * @see PreparedStatement#addBatch()
     * @see Statement#addBatch(String)
     * @see java.sql.PreparedStatement#executeLargeBatch()
     */
    Mono<List<Long>> executeLargeBatch();


    /**
     * Executes the SQL query in this <code>PreparedStatement</code> object
     * and returns the <code>ResultSet</code> object generated by the query.
     *
     * @return a <code>ResultSet</code> object that contains the data produced by the
     * query; never <code>null</code>
     * <p>
     * Mono throw {@link SQLException } if a database access error occurs;
     * this method is called on a closed  <code>PreparedStatement</code> or the SQL
     * statement does not return a <code>ResultSet</code> object
     * </p>
     * @see java.sql.PreparedStatement#executeQuery()
     */
    Flux<? extends ResultRow> executeQuery();
}
