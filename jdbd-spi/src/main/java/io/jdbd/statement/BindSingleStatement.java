package io.jdbd.statement;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface is base interface of followning:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link ServerPrepareStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see ServerPrepareStatement
 */
public interface BindSingleStatement extends ParameterStatement, BindMultiResultStatement {

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bindStmtVar(String name, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindSingleStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * @see BindStatement#addBatch()
     * @see PreparedStatement#addBatch()
     */
    BindSingleStatement addBatch() throws JdbdException;

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


    <R> Publisher<R> executeQuery(Function<CurrentRow, R> function);


    <R> Publisher<R> executeQuery(Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer);


}
