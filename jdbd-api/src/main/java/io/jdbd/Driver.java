package io.jdbd;

import java.util.Map;

public interface Driver {


    boolean acceptsUrl(String url);


    /**
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    DatabaseSessionFactory createSessionFactory(String url, Map<String, String> properties) throws JdbdNonSQLException;

    /**
     * <p>
     * This method is designed for poll session vendor developer,so application developer shouldn't invoke this method
     * and use {@link #createSessionFactory(String, Map)} method.
     * </p>
     *
     * <p>  This method returning {@link DatabaseSessionFactory} has below feature.
     *     <ul>
     *         <li>{@link DatabaseSessionFactory#getTxSession()} returning instance is {@link io.jdbd.pool.PoolTxDatabaseSession} instance</li>
     *         <li>{@link DatabaseSessionFactory#getXaSession()} returning instance is {@link io.jdbd.pool.PoolXaDatabaseSession} instance</li>
     *     </ul>
     * </p>
     *
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    DatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties) throws JdbdNonSQLException;


}
