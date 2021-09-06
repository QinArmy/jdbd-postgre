package io.jdbd.vendor.stmt;


import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * This interface representing {@link Stmt} have only one sql that has parameter placeholder and isn't batch.
 * The implementation of this interface is used by the implementation of below methods:
 * <u>
 * <li>{@link io.jdbd.stmt.PreparedStatement#executeUpdate()}</li>
 * <li>{@link io.jdbd.stmt.PreparedStatement#executeQuery()}</li>
 * <li>{@link io.jdbd.stmt.PreparedStatement#executeQuery(Consumer)}</li>
 * <li>{@link io.jdbd.stmt.BindStatement#executeUpdate()}</li>
 * <li>{@link io.jdbd.stmt.BindStatement#executeQuery()}</li>
 * <li>{@link io.jdbd.stmt.BindStatement#executeQuery(Consumer)}</li>
 * </u>
 * </p>
 */
public interface ParamStmt extends ParamSingleStmt, FetchAbleSingleStmt {

    /**
     * @return a unmodifiable list
     */
    List<? extends ParamValue> getBindGroup();


}
