package io.jdbd.stmt;

import io.jdbd.result.MultiResults;
import org.reactivestreams.Publisher;

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
    MultiResults executeMulti();

    /**
     * @see BindableStatement#executeBatchMulti()
     * @see PreparedStatement#executeBatchMulti()
     * @see MultiStatement#executeBatchMulti()
     */
    Publisher<MultiResults> executeBatchMulti();

}
