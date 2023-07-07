package io.jdbd.statement;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.session.DatabaseSession;
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
     * @param name     statement variable name,must have text.
     * @param dataType variable type
     * @param nullable nullable variable value
     * @return <strong>this</strong>
     * @throws JdbdException throw when <ul>
     *                       <li>{@link DatabaseSession#supportStmtVar()} or {@link #supportStmtVar()} return false</li>
     *                       <li>name have no text</li>
     *                       <li>name duplication</li>
     *                       <li>dataType is no supported by database</li>
     *                       <li>the java type is not supported by database</li>
     *                       <li>reuse this statement instance</li>
     *                       </ul>
     * @see DatabaseSession#supportStmtVar()
     * @see #supportStmtVar()
     */
    Statement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


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
