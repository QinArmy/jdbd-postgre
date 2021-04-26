package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.AbstractHostInfo;
import io.jdbd.vendor.conf.JdbcUrlParser;

public final class PostgreHost extends AbstractHostInfo<Property> {

    public static PostgreHost create(JdbcUrlParser parser, int index) {
        return new PostgreHost(parser, index);
    }


    public static final int DEFAULT_PORT = 5432;


    private PostgreHost(JdbcUrlParser parser, int index) {
        super(parser, index);
    }

    @Override
    protected final Property getUserKey() {
        return Property.user;
    }

    @Override
    protected final Property getPasswordKey() {
        return Property.password;
    }

    @Override
    protected final Property getHostKey() {
        return Property.PGHOST;
    }

    @Override
    protected final Property getPortKey() {
        return Property.PGPORT;
    }

    @Override
    protected final Property getDbNameKey() {
        return Property.PGDBNAME;
    }

    @Override
    protected final int getDefaultPort() {
        return DEFAULT_PORT;
    }


}
