package io.jdbd.postgre.env;

import io.jdbd.vendor.env.AbstractHostInfo;
import io.jdbd.vendor.env.JdbcUrlParser;
import io.jdbd.vendor.env.Properties;

import java.util.Map;

public class PgHost extends AbstractHostInfo {

    public static PgHost create(JdbcUrlParser parser, int index) {
        return new PgHost(parser, index);
    }


    public static final int DEFAULT_PORT = 5432;


    private PgHost(JdbcUrlParser parser, int index) {
        super(parser, index);
    }

    @Override
    protected final PgKey getUserKey() {
        return PgKey.user;
    }

    @Override
    protected final PgKey getPasswordKey() {
        return PgKey.password;
    }

    @Override
    protected final PgKey getHostKey() {
        return PgKey.PGHOST;
    }

    @Override
    protected final PgKey getPortKey() {
        return PgKey.PGPORT;
    }

    @Override
    protected final PgKey getDbNameKey() {
        return PgKey.PGDBNAME;
    }

    @Override
    protected final int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public String getNonNullDbName() {
        String dbName = this.dbName;
        return dbName == null ? "" : dbName;
    }


    @Override
    protected Properties createProperties(Map<String, String> map) {
        return PgUrl.wrapProperties(map);
    }


}
