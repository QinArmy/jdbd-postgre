package io.jdbd;

import io.jdbd.session.DatabaseSessionFactory;

import java.util.Map;

public interface Driver {


    /**
     * @param url jdbc url
     * @return true: accept
     * @throws NullPointerException when url is null
     */
    boolean acceptsUrl(String url);


    /**
     * @param url jdbc url
     * @throws UrlException         when url error.
     * @throws PropertyException    when properties error.
     * @throws NullPointerException when url or properties is null
     */
    DatabaseSessionFactory createSessionFactory(String url, Map<String, String> properties);

    /**
     * <p>
     * This method is designed for poll session vendor developer,so application developer shouldn't invoke this method
     * and use {@link #createSessionFactory(String, Map)} method.
     * </p>
     *
     * <p>  This method return {@link DatabaseSessionFactory} has below feature.
     *     <ul>
     *         <li>{@link DatabaseSessionFactory#getTxSession()} returning instance is {@link io.jdbd.pool.PoolTxDatabaseSession} instance</li>
     *         <li>{@link DatabaseSessionFactory#getXaSession()} returning instance is {@link io.jdbd.pool.PoolXaDatabaseSession} instance</li>
     *     </ul>
     * </p>
     *
     * @param url jdbc url
     * @throws UrlException      when url error.
     * @throws PropertyException when properties error.
     * @throws NullPointerException             when url or properties is null
     */
    DatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties);


}
