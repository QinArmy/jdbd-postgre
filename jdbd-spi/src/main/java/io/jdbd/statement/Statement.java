package io.jdbd.statement;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.session.DatabaseSession;
import io.jdbd.type.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;


/**
 * <p>
 * This interface is base interface of below interfaces:
 *     <ul>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 * <p>
 *     NOTE: {@link Statement} is auto close after you invoke executeXxx() method,or binding occur error,so
 *     {@link Statement} have no close() method.
 * </p>
 *
 * @see StaticStatement
 * @see BindStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface Statement {


    /**
     * <p>
     * Bind one value to statement variable : <ul>
     * <li>statement variable is send with the statement.</li>
     * <li>statement variable exist until statement execution ends, at which point the statement variable set is cleared.</li>
     * <li>While statement variable exist, they can be accessed on the server side.</li>
     * </ul>
     * </p>
     *
     * @param name     statement variable name,must have text.
     * @param dataType parameter type is following type : <ul>
     *                 <li>{@link io.jdbd.meta.JdbdType} generic sql type,this method convert {@link io.jdbd.meta.JdbdType} to appropriate {@link io.jdbd.meta.SQLType},if fail throw {@link  JdbdException}</li>
     *                 <li>{@link io.jdbd.meta.SQLType} database build-in type. It is defined by driver developer.</li>
     *                 <li>the {@link DataType} that application developer define type and it's {@link DataType#typeName()} is supported by database.
     *                       <ul>
     *                           <li>If {@link DataType#typeName()} is database build-in type,this method convert dataType to appropriate {@link io.jdbd.meta.SQLType} . now {@link DataType#isUserDefined()} return false, or throw {@link JdbdException}.</li>
     *                           <li>Else if database support user_defined type,then use dataType. now {@link DataType#isUserDefined()} return true.</li>
     *                           <li>Else throw {@link JdbdException}</li>
     *                       </ul>
     *                 </li>
     *                 </ul>
     * @param value    nullable the parameter value; be following type :
     *                 <ul>
     *                    <li>generic java type,for example : {@link Boolean} , {@link Integer} , {@link String} , byte[],{@link Integer[]} ,{@link java.time.LocalDateTime} , {@link java.time.Duration} ,{@link java.time.YearMonth} ,{@link java.util.BitSet},{@link java.util.List}</li>
     *                    <li>{@link Point} spatial point type</li>
     *                    <li>{@link Interval} the composite of {@link java.time.Period} and {@link java.time.Duration}</li>
     *                    <li>{@link Parameter} :
     *                        <ol>
     *                            <li>{@link Blob} long binary</li>
     *                            <li>{@link Clob} long string</li>
     *                            <li>{@link Text} long text</li>
     *                            <li>{@link BlobPath} long binary,if {@link BlobPath#isDeleteOnClose()} is true , driver will delete file on close,see {@link java.nio.file.StandardOpenOption#DELETE_ON_CLOSE}</li>
     *                            <li>{@link TextPath} long text,if {@link TextPath#isDeleteOnClose()} is true , driver will delete file on close,see {@link java.nio.file.StandardOpenOption#DELETE_ON_CLOSE}</li>
     *                        </ol>
     *                    </li>
     *                 </ul>
     * @return <strong>this</strong>
     * @throws JdbdException throw when : <ul>
     *                       <li>{@link DatabaseSession#supportStmtVar()} or {@link #supportStmtVar()} return false</li>
     *                       <li>this statement instance is reused.Because jdbd is reactive and multi-thread and jdbd provide :
     *                              <ol>
     *                                  <li>{@link MultiResultStatement#executeBatchUpdate()}</li>
     *                                  <li>{@link MultiResultStatement#executeBatchQuery()} </li>
     *                                  <li>{@link MultiResultStatement#executeBatchAsMulti()}</li>
     *                                  <li>{@link MultiResultStatement#executeBatchAsFlux()}</li>
     *                              </ol>
     *                              ,you don't need to reuse statement instance.
     *                       </li>
     *                       <li>name have no text</li>
     *                       <li>name duplication</li>
     *                       <i>dataType is {@link io.jdbd.meta.JdbdType#UNKNOWN} or {@link io.jdbd.meta.JdbdType#DIALECT_TYPE}</i>
     *                       <li>dataType is null or dataType is supported by database.</li>
     *                       <li>the java type of value isn't supported by appropriate dataType</li>
     *                       </ul>
     * @see DatabaseSession#supportStmtVar()
     * @see #supportStmtVar()
     * @see io.jdbd.meta.JdbdType
     * @see io.jdbd.meta.SQLType
     * @see Point
     * @see Blob
     * @see Clob
     * @see Text
     * @see BlobPath
     * @see TextPath
     */
    Statement bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException;


    /**
     * <p>
     * long data at least contains below two type.
     * <ul>
     *     <li>{@link java.nio.file.Path}</li>
     *     <li><{@link org.reactivestreams.Publisher}/li>
     * </ul>
     * </p>
     *
     * @return true : support
     */
    boolean supportPublisher();

    boolean supportOutParameter();

    boolean supportStmtVar();

    Statement setTimeout(int seconds);


    /**
     * <p>
     * Only below methods support this method:
     *     <ul>
     *         <li>{@code #executeQuery()}</li>
     *         <li>{@code #executeQuery(Consumer)}</li>
     *     </ul>
     * </p>
     * <p>
     * invoke before invoke {@code #executeQuery()} or {@code #executeQuery(Consumer)}.
     * </p>
     *
     * @param fetchSize fetch size ,positive support
     */
    Statement setFetchSize(int fetchSize) throws JdbdException;

    Statement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    Statement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


    DatabaseSession getSession();

    <T extends DatabaseSession> T getSession(Class<T> sessionClass);

}
