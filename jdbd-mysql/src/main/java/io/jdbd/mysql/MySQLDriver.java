package io.jdbd.mysql;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.MySQLDatabaseSessionFactory;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.util.DefaultDriverVersion;

import java.util.Map;
import java.util.Objects;

public final class MySQLDriver implements Driver {

    private static final MySQLDriver INSTANCE = new MySQLDriver();

    public static Driver getInstance() {
        return INSTANCE;
    }

    private MySQLDriver() {
    }

    @Override
    public boolean acceptsUrl(final String url) {
        Objects.requireNonNull(url, "url");
        return MySQLUrl.acceptsUrl(url);
    }

    @Override
    public DatabaseSessionFactory createSessionFactory(String url, Map<String, Object> properties)
            throws JdbdException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.forPoolVendor(url, properties);
    }

    @Override
    public DriverVersion getVersion() {
        return VersionHolder.VERSION;
    }


    private static final class VersionHolder {

        private static final DriverVersion VERSION;

        static {
            VERSION = DefaultDriverVersion.from(MySQLDriver.class.getName(), MySQLDriver.class);
        }

    }


}
