package io.jdbd.stmt;


/**
 * <p>
 * This interface representing this {@link Stmt} only one sql.
 * </p>
 * <p>
 * This is a base interface of :
 *     <ul>
 *         <li>{@link StaticStmt}</li>
 *         <li>{@link ParamStmt}</li>
 *         <li>{@link ParamBatchStmt}</li>
 *     </ul>
 * </p>
 */
public interface SingleStmt extends Stmt {

    String getSql();


}
