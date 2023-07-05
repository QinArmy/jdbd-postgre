package io.jdbd.env;

import reactor.util.annotation.Nullable;

/**
 * @see JdbcUrl
 */
public interface HostInfo {

    String HOST_PORT_SEPARATOR = ":";

    String DEFAULT_HOST = "localhost";

    String getOriginalUrl();

    String getHostPortPair();


    String getHost();

    int getPort();

    String getUser();

    @Nullable
    String getPassword();

    boolean isPasswordLess();

    Properties getProperties();

    @Nullable
    String getDbName();

    @Override
    String toString();

}
