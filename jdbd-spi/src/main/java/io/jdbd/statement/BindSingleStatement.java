package io.jdbd.statement;


import io.jdbd.JdbdException;
import io.jdbd.result.*;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface is base interface of followning:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see PreparedStatement
 */
public interface BindSingleStatement extends ParameterStatement ,BindMultiResultStatement{


    /**
     * @see BindStatement#addBatch()
     * @see PreparedStatement#addBatch()
     */
    BindSingleStatement addBatch() throws JdbdException;

    /**
     * @see BindStatement#executeUpdate()
     * @see PreparedStatement#executeUpdate()
     */
    Publisher< ResultStates> executeUpdate();

    /**
     * @see BindStatement#executeQuery()
     * @see PreparedStatement#executeQuery()
     */

  default    Publisher<ResultRow> executeQuery(){
        throw new UnsupportedOperationException();
    }

    /**
     * @see BindStatement#executeQuery(Consumer)
     * @see PreparedStatement#executeQuery(Consumer)
     */
   default   Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer){
        throw new UnsupportedOperationException();
    }

    default <R> Publisher<R> executeQuery(Function< CurrentRow, R> function) {
        throw new UnsupportedOperationException();
    }


    /**
     * @see BindStatement#executeQuery(Consumer)
     * @see PreparedStatement#executeQuery(Consumer)
     */
    default <R> Publisher<R> executeQuery(Function< CurrentRow, R> function, Consumer< ResultStates> statesConsumer) {
        throw new UnsupportedOperationException();
    }



}
