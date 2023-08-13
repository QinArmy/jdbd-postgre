package io.jdbd.statement;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.OutResultItem;

/**
 * <p>
 * This interface representing out parameter of stored procedure.
 * You create instance of {@link InOutParameter} by {@link InOutParameter#from(String, Object)}.
 * </p>
 * <p>
 * Out parameter is usually supported by following statement :
 *     <ul>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link BindStatement}</li>
 *     </ul>
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
 * @see io.jdbd.meta.JdbdType#OUT
 * @see ParametrizedStatement#bind(int, DataType, Object)
 * @see OutResultItem
 * @since 1.0
 */
public interface InOutParameter extends Parameter {


    /**
     * @return out parameter value.
     */
    @Nullable
    Object value();

    /**
     * override {@link Object#hashCode()}
     */
    @Override
    int hashCode();

    /**
     * override {@link Object#equals(Object)}
     */
    @Override
    boolean equals(Object obj);

    /**
     * override {@link Object#toString()}
     *
     * @return out parameter info, contain {@link System#identityHashCode(Object)},
     * but if {@link #value()} is {@link String} , then always output {@code ?} ,because of information safe.
     */
    @Override
    String toString();


    /**
     * @throws NullPointerException     throw when name is null .
     * @throws IllegalArgumentException throw when value is the instance of {@link InOutParameter}.
     */
    static InOutParameter from(String name, @Nullable Object value) {
        return JdbdParameters.outParam(name, value);
    }


}
