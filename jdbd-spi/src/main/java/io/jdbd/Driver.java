package io.jdbd;

import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;

import java.util.Map;


/**
 * <p>
 * This interface representing database driver. This interface is implemented by database vendor.
 * The implementation of this interface must provide public static factory method, like following :
 * <pre><br/>
 *   public static Driver getInstance() {
 *       return INSTANCE;
 *   }
 *  </pre>
 * </p>
 *
 * @see DriverManager
 * @since 1.0
 */
public interface Driver {

    String USER = "user";

    String PASSWORD = "password";


    /**
     * @param url jdbc url
     * @return true: accept
     * @throws NullPointerException when url is null
     */
    boolean acceptsUrl(String url);


    /**
     * @param url jdbc url
     */
    DatabaseSessionFactory createSessionFactory(String url, Map<String, Object> properties) throws JdbdException;

    /**
     * <p>
     * This method is designed for poll session vendor developer,so application developer shouldn't invoke this method
     * and use {@link #createSessionFactory(String, Map)} method.
     * </p>
     *
     * <p>  This method return {@link DatabaseSessionFactory} has below feature.
     *     <ul>
     *         <li>{@link DatabaseSessionFactory#localSession()} returning instance is {@link   io.jdbd.pool.PoolLocalDatabaseSession} instance</li>
     *         <li>{@link DatabaseSessionFactory#rmSession()} returning instance is {@link  PoolRmDatabaseSession} instance</li>
     *     </ul>
     * </p>
     * <p>
     *     This method is used by pool vendor,application developer shouldn't use this method.
     *     <strong>NOTE</strong> : driver developer are not responsible for pooling.
     * </p>
     *
     * @param url jdbc url
     */
    DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties) throws JdbdException;

    DriverVersion getVersion();

}
