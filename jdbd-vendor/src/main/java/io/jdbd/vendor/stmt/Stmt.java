package io.jdbd.vendor.stmt;

/**
 * <p>
 * This interface representing object that wrap sql and parameter and option(eg: timeout).
 * The implementation of this interface is used by the implementation of {@link io.jdbd.statement.Statement}
 * </p>
 * <p>
 * This interface is a base interface of :
 *     <ul>
 *         <li>{@link StaticStmt}</li>
 *         <li>{@link StaticBatchStmt}</li>
 *         <li>{@link ParamStmt}</li>
 *         <li>{@link ParamBatchStmt}</li>
 *         <li>{@link ParamMultiStmt}</li>
 *     </ul>
 * </p>
 * </p>
 */
public interface Stmt extends StmtOption {


    /**
     * @return false : {@link #databaseSession()} always throw {@link UnsupportedOperationException}
     */
    boolean isSessionCreated();

}
