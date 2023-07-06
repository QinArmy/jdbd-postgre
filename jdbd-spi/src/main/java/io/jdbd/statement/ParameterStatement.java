package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link OneStepPrepareStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface ParameterStatement extends Statement {


    /**
     * <p>
     * Bind parameter value to statement that exists SQL parameter placeholder and SQL parameter placeholder must be {@code ?}
     * </p>
     * <p>
     * dataType is following type : <ul>
     * <li>{@link io.jdbd.meta.JdbdType} generic sql type,this method convert {@link io.jdbd.meta.JdbdType} to {@link io.jdbd.meta.SQLType},if failure throw {@link  JdbdException}</li>
     * <li>{@link io.jdbd.meta.SQLType} database build-in type. It is defined by driver developer.</li>
     * <li>the {@link DataType} that application developer define type and it's {@link DataType#typeName()} is supported by database.
     *       <ul>
     *           <li>If {@link DataType#typeName()} is database build-in type,this method convert dataType to {@link io.jdbd.meta.SQLType} . now {@link DataType#isUserDefined()} return false.</li>
     *           <li>Else if database support user_defined type,then use dataType. now {@link DataType#isUserDefined()} return true.</li>
     *           <li>Else throw {@link JdbdException}</li>
     *       </ul>
     * </li>
     * </ul>
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param dataType       parameter type.
     */
    ParameterStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ParameterStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ParameterStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParameterStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParameterStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParameterStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
