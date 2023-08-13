package io.jdbd.result;

import io.jdbd.statement.InOutParameter;

/**
 * <p>
 * This interface representing out parameter of stored procedure, that is returned by database server.
 * </p>
 * <p>
 * For example :
 * <pre>
 *     <code><br/>
 *       // PostgreSQL
 *       CREATE  PROCEDURE my_test_procedure( IN my_input INT, OUT my_out INT, INOUT INT my_inout)
 *           LANGUAGE plpgsql
 *       AS $$
 *
 *       BEGIN
 *           my_out = my_input + 1;
 *           my_inout = my_inout + 8888;
 *       END;
 *       $$;
 *        <br/>
 *        LocalDatabaseSession session;
 *        BindStatement stmt = session.bindStatement("CALL my_test_procedure( ? , ? , ?)");
 *        stmt.bind(0,JdbdType.INTEGER,1);
 *        stmt.bind(1,JdbdType.OUT,null); // the value of JdbdType.OUT must be null.
 *        stmt.bind(2,JdbdType.INTEGER,InOutParameter.from("my_inout",6666)); //
 *
 *        Flux.from(stmt.executeQuery())
 *              //.filter(ResultItem::isOutResultItem) // actually , here don't need filter,  because the sql produce just one result.
 *              .map(this::handleOutParameter)
 *
 *       private Map&lt;String, Integer> handleOutParameter(final ResultRow row) {
 *           Map&lt;String, Integer> map = new HashMap&lt;>(4);
 *           map.put(row.getColumnLabel(0), row.get(0, Integer.class));
 *           map.put(row.getColumnLabel(1), row.get(1, Integer.class));
 *           return map;
 *       }
 *     </code>
 * </pre>
 * </p>
 *
 * @see InOutParameter
 * @see io.jdbd.meta.JdbdType#OUT
 * @since 1.0
 */
public interface OutResultItem extends ResultItem {


}
