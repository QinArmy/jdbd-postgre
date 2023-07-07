package io.jdbd.vendor.env;

public interface JdbdHost {

    String HOST_PORT_SEPARATOR = ":";

    String DEFAULT_HOST = "localhost";

    String getHost();

    int getPort();


}
