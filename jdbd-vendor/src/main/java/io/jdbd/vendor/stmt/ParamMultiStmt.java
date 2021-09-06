package io.jdbd.vendor.stmt;

import java.util.List;

/**
 * <p>
 * This interface representing multi {@link ParamStmt}.
 * This used by {@link io.jdbd.stmt.MultiStatement} for wrap sql and params.
 * </p>
 */
public interface ParamMultiStmt extends Stmt {

    List<? extends ParamStmt> getStmtGroup();

}
