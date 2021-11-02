package io.jdbd.stmt;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see PreparedStatement
 */
public interface BindSingleStatement extends Statement {


    /**
     * @see BindStatement#bind(int, Object)
     * @see PreparedStatement#bind(int, Object)
     */
    void bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * @see BindStatement#addBatch()
     * @see PreparedStatement#addBatch()
     */
    void addBatch() throws JdbdException;

    /**
     * @see BindStatement#executeUpdate()
     * @see PreparedStatement#executeUpdate()
     */
    Publisher<ResultStates> executeUpdate();

    /**
     * @see BindStatement#executeQuery()
     * @see PreparedStatement#executeQuery()
     */
    Publisher<ResultRow> executeQuery();

    /**
     * @see BindStatement#executeQuery(Consumer)
     * @see PreparedStatement#executeQuery(Consumer)
     */
    Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

    /**
     * @see BindStatement#executeBatchUpdate()
     * @see PreparedStatement#executeBatchUpdate()
     */
    Publisher<ResultStates> executeBatchUpdate();

    /**
     * @see BindStatement#executeBatchAsMulti()
     * @see PreparedStatement#executeBatchAsMulti()
     */
    MultiResult executeBatchAsMulti();

    /**
     * @see BindStatement#executeBatchAsFlux()
     * @see PreparedStatement#executeBatchAsFlux()
     */
    OrderedFlux executeBatchAsFlux();

}
