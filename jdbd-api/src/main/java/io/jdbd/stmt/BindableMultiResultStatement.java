package io.jdbd.stmt;

import io.jdbd.result.MultiResults;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindableStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 * <p>
 *     This interface only definite one method {@link #execute()},don't add new method in the future.
 * </p>
 *
 * @see BindableStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface BindableMultiResultStatement extends Statement {

    /**
     * @see BindableStatement#execute()
     * @see PreparedStatement#execute()
     * @see MultiStatement#execute()
     */
    MultiResults execute();

}
