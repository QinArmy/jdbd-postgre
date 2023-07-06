package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

public interface MultiStatement extends BindMultiResultStatement,ParameterStatement {

    /**
     * @param sql must have text.
     * @throws IllegalArgumentException when sql has no text.
     * @throws JdbdException            when reuse this instance after invoke below method:
     *                                  <ul>
     *                                      <li>{@link #executeBatchUpdate()}</li>
     *                                      <li>{@link #executeBatchAsMulti()}</li>
     *                                      <li>{@link #executeBatchAsFlux()}</li>
     *                                  </ul>
     */
    MultiStatement addStatement(String sql) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
