package io.jdbd.mysql.env;

import io.jdbd.lang.Nullable;
import io.jdbd.vendor.env.JdbdHost;

public interface MySQLHostEnv extends Environment, JdbdHost {

    @Nullable
    String getDbName();

}
