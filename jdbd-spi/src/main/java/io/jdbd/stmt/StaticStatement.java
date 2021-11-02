package io.jdbd.stmt;

import io.jdbd.JdbdSQLException;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SessionCloseException;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.Statement}
 * </p>
 */
public interface StaticStatement extends Statement {


    /**
     * Executes the given SQL statement(no parameter placeholder) thant can only producer one update result.
     * for example :
     * <ul>
     *     <li>INSERT</li>
     *     <li>UPDATE</li>
     *     <li>DELETE</li>
     *     <li>CREATE TABLE</li>
     *     <li>CALL Stored procedures that just produce one update result and no out parameter.</li>
     * </ul>
     * this method like {@code java.sql.Statement#executeUpdate(String)}
     * <p>
     *     Below are correct examples:
     *     <pre>
     *         <code>
     *              //correct example 1:
     *              String sql == "INSERT INTO user(name,age) VALUE('qinarmy',1)";
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *         </code>
     *         <code>
     *              //correct example 2:
     *              String sql == "UPDATE user as u SET u.name = 'qin' WHERE u.id = 1";
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *         </code>
     *         <code>
     *              //correct example 3:
     *              StaticStatement stmt = databaseSession.statement();
     *              stmt.setExecuteTimeout(10) // you can reuse stmt ,if you don't invoke any setXxx method again.
     *
     *              CREATE PROCEDURE single_result_procedure()
     *              BEGIN
     *                  UPDATE user as u SET u.name = 'qin' WHERE u.id = 1
     *              END;
     *
     *              String sql1 == "CALL single_result_procedure()";
     *              String sql2 == "UPDATE user as u SET u.name = 'qin' WHERE u.id = 1";
     *              Mono.from(stmt.executeUpdate(sql1))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .map(this::handleFirstUpdate) // handleFirstUpdate is a method
     *                  .then( Mono.from(stmt.executeUpdate(sql2)) ) // you can directly use 'then' method,because executeUpdate method return a deferred publisher.
     *                  .map(this::handleSecondUpdate) // handleSecondUpdate is a method
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *              // you don't need consider stmt close after execution.
     *         </code>
     *         <code>
     *              //correct example 4:
     *              StaticStatement stmt = databaseSession.statement();
     *              stmt.setExecuteTimeout(10) // you can reuse stmt ,if you don't invoke any setXxx method again.
     *
     *              String sql1 == "INSERT INTO user(name,age) VALUE('qinarmy',1)";
     *              String sql2 == "UPDATE user as u SET u.name = 'qin' WHERE u.id = 1";
     *              Mono.from(stmt.executeUpdate(sql1))
     *                  .map(this::handleFirstUpdate) // handleFirstUpdate is a method
     *                  .then( Mono.from(stmt.executeUpdate(sql2)) ) // you can directly use 'then' method,because executeUpdate method return a deferred publisher.
     *                  .map(this::handleSecondUpdate) // handleSecondUpdate is a method
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *               // you don't need consider stmt close after execution.
     *         </code>
     *     </pre>
     *
     * </p>
     *
     * <p>
     *     <pre>
     *     Below are error examples:
     *         <code>
     *              //error example 1:
     *              String sql == "INSERT INTO user(name,age) VALUE('qinarmy',?)"; // can't execute sql that contain any parameter placeholder.
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *             //above emit JdbdSQLException
     *         </code>
     *         <code>
     *              //error example 2:
     *              String sql == "SELECT u.name FROM user as u LIMIT 1"; // can't execute any sql that produce query result.
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *             //above emit SubscribeException
     *         </code>
     *         <code>
     *              //error example 3:
     *              CREATE PROCEDURE multi_result_procedure()
     *              BEGIN
     *                  UPDATE user as u SET u.name = 'qin' WHERE u.id = 1
     *                  UPDATE user as u SET u.name = 'qin' WHERE u.id = 2
     *              END;
     *
     *              String sql == "CALL multi_result_procedure()"; // can't execute any sql that produce multi-result.
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *              //above emit SubscribeException
     *         </code>
     *         <code>
     *              //error example 4:
     *              CREATE PROCEDURE single_result_procedure(out rows INT)
     *              BEGIN
     *                   SELECT 1 INTO rows;
     *              END;
     *
     *              String sql == "CALL single_result_procedure()"; // can't execute sql that producer out parameter
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *              //above emit SubscribeException
     *         </code>
     *         <code>
     *              //error example 5:
     *              String sql == "UPDATE user as u SET u.name = 'qin' WHERE u.id = 1 ; INSERT INTO user(name,age) VALUE('qinarmy',1)";
     *              // can't execute any sql that is multi-statement
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *             //above emit JdbdSQLException
     *         </code>
     *         <code>
     *              //error example 6:
     *              String sql == "INSERT INTO user(name,age) VALUE('qinarmy',1) RETURNING * "; // can't execute sql that producer any column.
     *              Mono.from(stmt.executeUpdate(sql))  // stmt is io.jdbd.stmt.StaticStatement instance.
     *                  .subscribe() // if no subscribe ,don't communication with database server
     *             //above emit SubscribeException
     *         </code>
     *     </pre>
     * </p>
     *
     * @param sql sql thant can only producer one update result.
     * @return a deferred publisher that emit at most one element, like {@code reactor.core.publisher.Mono},
     * no communication with database server util subscribe.
     * @throws SessionCloseException       emit when {@link DatabaseSession} has closed.
     * @throws SubscribeException          emit when sql produce query result or multi-result.
     * @throws JdbdSQLException            emit when sql execution occur error.
     * @throws io.jdbd.JdbdNonSQLException emit when if occur other error.
     */
    Publisher<ResultStates> executeUpdate(String sql);

    /**
     * @see #executeQuery(String, Consumer)
     */
    Publisher<ResultRow> executeQuery(String sql);

    /**
     * Executes the SQL query in this <code>PreparedStatement</code> object
     * and returns the <code>ResultSet</code> object generated by the query.
     *
     * @return a <code>ResultSet</code> object that contains the data produced by the
     * query; never <code>null</code>
     * <p>
     * Flux throw {@link JdbdSQLException } if a database access error occurs;
     * this method is called on a closed  <code>PreparedStatement</code> or the SQL
     * statement does not return a <code>ResultSet</code> object
     * </p>
     */
    Publisher<ResultRow> executeQuery(String sql, Consumer<ResultStates> statesConsumer);


    Publisher<ResultStates> executeBatchUpdate(List<String> sqlGroup);


    MultiResult executeBatchAsMulti(List<String> sqlGroup);

    OrderedFlux executeBatchAsFlux(List<String> sqlGroup);

    OrderedFlux executeAsFlux(String multiStmt);


}
