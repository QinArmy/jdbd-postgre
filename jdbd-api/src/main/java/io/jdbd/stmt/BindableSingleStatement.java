package io.jdbd.stmt;


import io.jdbd.lang.Nullable;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindableStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindableStatement
 * @see PreparedStatement
 */
public interface BindableSingleStatement extends Statement {

    /**
     * @see BindableStatement#bind(int, Object)
     * @see PreparedStatement#bind(int, Object)
     */
    void bind(int indexBasedZero, @Nullable Object nullable);

    /**
     * @see BindableStatement#addBatch()
     * @see PreparedStatement#addBatch()
     */
    void addBatch();

    /**
     * @see BindableStatement#executeBatch()
     * @see PreparedStatement#executeBatch()
     */
    Publisher<ResultStatus> executeBatch();

    /**
     * @see BindableStatement#executeUpdate()
     * @see PreparedStatement#executeUpdate()
     */
    Publisher<ResultStatus> executeUpdate();

    /**
     * @see BindableStatement#executeQuery()
     * @see PreparedStatement#executeQuery()
     */
    Publisher<ResultRow> executeQuery();

    /**
     * @see BindableStatement#executeQuery(Consumer)
     * @see PreparedStatement#executeQuery(Consumer)
     */
    Publisher<ResultRow> executeQuery(Consumer<ResultStatus> statesConsumer);

}
