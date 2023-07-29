package io.jdbd.vendor.env;

import io.jdbd.env.JdbdEnvironment;
import reactor.util.annotation.Nullable;

/**
 * @see JdbcUrl
 */
@Deprecated
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

    @Deprecated
    Properties getProperties();

    JdbdEnvironment getEnvironment();

    @Nullable
    String getDbName();

    @Override
    String toString();

}
