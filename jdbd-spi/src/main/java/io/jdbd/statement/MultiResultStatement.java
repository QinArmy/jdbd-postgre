package io.jdbd.statement;

import io.jdbd.result.BatchQuery;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface MultiResultStatement extends Statement {


    Publisher<ResultStates> executeBatchUpdate();

    BatchQuery executeBatchQuery();

    /**
     * @see BindStatement#executeBatchAsMulti()
     * @see PreparedStatement#executeBatchAsMulti()
     * @see MultiStatement#executeBatchAsMulti()
     */
    MultiResult executeBatchAsMulti();

    /**
     * @see BindStatement#executeBatchAsMulti()
     * @see PreparedStatement#executeBatchAsMulti()
     * @see MultiStatement#executeBatchAsMulti()
     */
    OrderedFlux executeBatchAsFlux();

}
