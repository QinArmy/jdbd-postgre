package io.jdbd.postgre.env;

import io.jdbd.vendor.env.JdbdHost;

import java.util.Map;

/**
 * <p>
 * This interface a postgre host from url.
 * </p>
 * <p>
 * The instance of interface is created by {@link PgUrlParser#parse(String, Map)} method.
 * </p>
 *
 * @since 1.0
 */
public interface PgHost extends JdbdHost.HostInfo {

    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">Postgre url</a>
     */
    int DEFAULT_PORT = 5432;


}
