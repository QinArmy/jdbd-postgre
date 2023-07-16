package io.jdbd.vendor.env;

import io.jdbd.lang.Nullable;

public interface JdbdHost {

    String HOST_PORT_SEPARATOR = ":";

    String DEFAULT_HOST = "localhost";

    String getHost();

    int getPort();


    interface HostInfo extends JdbdHost {
        String getUser();

        @Nullable
        String getPassword();

        @Nullable
        String getDbName();
    }


}
