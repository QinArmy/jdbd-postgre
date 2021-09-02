package io.jdbd.stmt;


import io.jdbd.DatabaseSession;
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
 *
 * @see StaticStatement
 * @see BindStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface Statement {

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

    void setTimeout(int seconds);


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
     * @return true :<ul>
     * <li>fetchSize great than zero</li>
     * <li>driver implementation support fetch</li>
     * </ul>
     */
    boolean setFetchSize(int fetchSize);

    boolean setImportPublisher(Function<Object, Publisher<byte[]>> function);

    boolean setExportSubscriber(Function<Object, Subscriber<byte[]>> function);


    DatabaseSession getSession();

    <T extends DatabaseSession> T getSession(Class<T> sessionClass);

}
