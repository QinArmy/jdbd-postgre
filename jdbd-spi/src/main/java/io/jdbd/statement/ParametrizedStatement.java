package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.OutResult;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface representing parametrized statement that SQL parameter placeholder must be {@code ?} .
 * </p>
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface ParametrizedStatement extends Statement {


    /**
     * <p>
     * Bind parameter value to statement that exists SQL parameter placeholder and SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero, the first value is 0 .
     * @param dataType       parameter type is following type : <ul>
     *                       <li>{@link io.jdbd.meta.JdbdType} generic sql type,this method convert {@link io.jdbd.meta.JdbdType} to {@link io.jdbd.meta.SQLType},if failure throw {@link  JdbdException}</li>
     *                       <li>{@link io.jdbd.meta.SQLType} database build-in type. It is defined by driver developer.</li>
     *                       <li>the {@link DataType} that application developer define type and it's {@link DataType#typeName()} is supported by database.
     *                             <ul>
     *                                 <li>If {@link DataType#typeName()} is database build-in type,this method convert dataType to {@link io.jdbd.meta.SQLType} . now {@link DataType#isUserDefined()} return false.</li>
     *                                 <li>Else if database support user_defined type,then use dataType. now {@link DataType#isUserDefined()} return true.</li>
     *                                 <li>Else throw {@link JdbdException}</li>
     *                             </ul>
     *                       </li>
     *                       </ul>
     * @param nullable       nullable the parameter value; should be following type :
     *                       <ul>
     *                          <li>generic java type,for example : {@link Boolean} , {@link Integer} , {@link String} , byte[], {@link java.time.LocalDateTime} ,{@link java.util.BitSet}</li>
     *                          <li>{@link Publisher}  long binary, it must emit byte[]</li>
     *                          <li>{@link java.nio.file.Path} long binary</li>
     *                          <li>{@link Parameter} :
     *                              <ol>
     *                                  <li>{@link OutParameter} that representing out parameter of stored procedure,see {@link  OutResult}</li>
     *                                  <li>{@link Blob} long binary</li>
     *                                  <li>{@link Clob} long string</li>
     *                                  <li>{@link Text} long text</li>
     *                                  <li>{@link BlobPath} long binary</li>
     *                                  <li>{@link TextPath} long text</li>
     *                              </ol>
     *                          </li>
     *                       </ul>
     * @return <strong>this</strong>
     * @see io.jdbd.meta.JdbdType
     * @see io.jdbd.meta.SQLType
     * @see OutParameter
     * @see Blob
     * @see Clob
     * @see Text
     * @see BlobPath
     * @see TextPath
     */
    ParametrizedStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ParametrizedStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ParametrizedStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParametrizedStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParametrizedStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ParametrizedStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
