package io.jdbd.vendor.conf;

import reactor.util.annotation.Nullable;

/**
 * @param <K> database vendor property key type.
 * @see JdbcUrl
 */
public interface HostInfo<K extends IPropertyKey> {

    String HOST_PORT_SEPARATOR = ":";

    String DEFAULT_HOST = "localhost";

    String HOST = "host";

    String PORT = "port";

    String USER = "user";

    String PASSWORD = "password";

    String DB_NAME = "dbname";

    String getOriginalUrl();

    String getHostPortPair();


    String getHost();

    int getPort();

    String getUser();

    @Nullable
    String getPassword();

    boolean isPasswordLess();

    Properties<K> getProperties();

    @Nullable
    String getDbName();

    @Override
    String toString();

}
