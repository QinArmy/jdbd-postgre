package io.jdbd.stmt;

import io.jdbd.result.MultiResult;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindableStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * <p>
 *     This interface only definite {@link #executeMulti()} method,don't add new method in the future
 *     ,because {@link MultiStatement} isn't compatible.
 * </p>
 *
 * @see BindableStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface BindableMultiResultStatement extends Statement {

    /**
     * @see BindableStatement#executeMulti()
     * @see PreparedStatement#executeMulti()
     * @see MultiStatement#executeMulti()
     */
    MultiResult executeMulti();

}
