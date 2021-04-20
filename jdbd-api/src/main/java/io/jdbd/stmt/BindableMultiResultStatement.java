package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.SingleResult;
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

    void bind(int index, @Nullable Object nullable);

    /**
     * @see BindableStatement#executeAsMulti()
     * @see PreparedStatement#executeAsMulti()
     * @see MultiStatement#executeAsMulti()
     */
    MultiResult executeAsMulti();

    /**
     * @see BindableStatement#executeAsMulti()
     * @see PreparedStatement#executeAsMulti()
     * @see MultiStatement#executeAsMulti()
     */
    Publisher<SingleResult> executeAsFlux();

}
