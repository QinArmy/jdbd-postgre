package io.jdbd.postgre;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.postgre.session.PgDatabaseSessionFactory;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.util.DefaultDriverVersion;

import java.util.Map;
import java.util.Objects;

public final class PgDriver implements Driver {

    private static final PgDriver INSTANCE = new PgDriver();

    public static PgDriver getInstance() {
        return INSTANCE;
    }


    private PgDriver() {
    }


    @Override
    public boolean acceptsUrl(String url) {
        Objects.requireNonNull(url, "url");
        return PgDatabaseSessionFactory.acceptsUrl(url);
    }

    @Override
    public DatabaseSessionFactory createSessionFactory(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return PgDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return PgDatabaseSessionFactory.forPoolVendor(url, properties);
    }


    public static DriverVersion getVersion() {
        return VersionHolder.VERSION;
    }


    private static final class VersionHolder {

        private static final DriverVersion VERSION;

        static {
            VERSION = DefaultDriverVersion.from(PgDriver.class.getName(), PgDriver.class);
        }

    }

}
