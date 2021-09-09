package io.jdbd.vendor.stmt;


/**
 * <p>
 * This interface representing adapter of:
 *     <ul>
 *         <li>{@link ParamStmt}</li>
 *         <li>{@link ParamBatchStmt}</li>
 *     </ul>
 *     The implementation of this interface is used by underlying implementation of {@link io.jdbd.stmt.PreparedStatement}.
 * </p>
 *
 * @see io.jdbd.DatabaseSession#prepare(String)
 */
public interface PrepareStmt extends ParamSingleStmt {

    /**
     * @throws IllegalStateException when no actual {@link ParamSingleStmt}
     */
    ParamSingleStmt getStmt();


}
