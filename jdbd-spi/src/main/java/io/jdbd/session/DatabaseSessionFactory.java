package io.jdbd.session;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import org.reactivestreams.Publisher;


/**
 * <p>
 * This interface representing the factory that create {@link DatabaseSession} by following methods:
 *     <ul>
 *         <li>{@link #localSession()}</li>
 *         <li>{@link #rmSession()}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface DatabaseSessionFactory extends OptionSpec, Closeable {

    /**
     * @return factory name. see {@link io.jdbd.Driver#FACTORY_NAME}
     */
    String name();

    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     */
    Publisher<LocalDatabaseSession> localSession();

    /**
     * <p>
     * Create one session that support XA transaction.
     * </p>
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     *
     * @throws io.jdbd.JdbdException emit(not throw) when
     *                               <ul>
     *                                   <li>{@link #isSupportXaTransaction()} return false</li>
     *                                   <li>network error</li>
     *                                   <li>server response error message,see {@link io.jdbd.result.ServerException}</li>
     *                               </ul>
     */
    Publisher<RmDatabaseSession> rmSession();


    boolean isSupportXaTransaction();


    /**
     * @return database product name,For example :  MySQL , PostgreSQL.
     */
    String productName();


    /**
     * @return session factory vendor,The value returned typically is the package name for this vendor.
     * The session factory vendor possibly is pool vendor.
     */
    String factoryVendor();

    /**
     * @return driver vendor,The value returned typically is the package name for this vendor.
     * @see Driver#vendor()
     */
    String driverVendor();

    /**
     * @see Driver#version()
     */
    DriverVersion driverVersion();

    /**
     * override {@link Object#toString()}
     *
     * @return driver info, contain : <ol>
     * <li>{@link #name()}</li>
     * <li>{@link #factoryVendor()}</li>
     * <li>{@link #driverVersion()}</li>
     * <li>{@link #productName()}</li>
     * <li>{@link #driverVersion()}</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();


}
