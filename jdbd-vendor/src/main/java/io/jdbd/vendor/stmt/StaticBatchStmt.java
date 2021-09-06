package io.jdbd.vendor.stmt;

import java.util.List;

/**
 * <p>
 * This interface representing stmt have only multi sql,and no parameter placeholder.
 * This implementation of this interface is used by the implementation of below methods:
 * <u>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeUpdate(String)}</li>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String)}</li>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String, java.util.function.Consumer)}</li>
 * </u>
 * </p>
 */
public interface StaticBatchStmt extends Stmt {

    List<String> getSqlGroup();

}
