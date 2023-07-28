package io.jdbd.vendor.env;

import io.jdbd.lang.Nullable;

public interface JdbdHost {

    String DEFAULT_HOST = "localhost";

    String host();

    int port();


    interface HostInfo extends JdbdHost {
        String user();

        @Nullable
        String password();

        @Nullable
        String dbName();
    }


}
