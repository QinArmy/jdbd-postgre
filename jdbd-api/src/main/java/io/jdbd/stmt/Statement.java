package io.jdbd.stmt;


import io.jdbd.DatabaseSession;

/**
 * <p>
 * This interface is base interface of below interfaces:
 *     <ul>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindableStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @see StaticStatement
 * @see BindableStatement
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
    boolean supportLongData();

    boolean supportOutParameter();

    DatabaseSession getSession();

    <T extends DatabaseSession> T getSession(Class<T> sessionClass);

}
