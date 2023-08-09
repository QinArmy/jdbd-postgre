package io.jdbd.postgre;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.postgre.session.PgDatabaseSessionFactory;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.util.DefaultDriverVersion;

import java.util.Map;

public final class PgDriver implements Driver {

    private static final PgDriver INSTANCE = new PgDriver();

    public static PgDriver getInstance() {
        return INSTANCE;
    }

    public static final String POSTGRE_SQL = "PostgreSQL";

    public static final String DRIVER_VENDOR = "io.jdbd.postgre";


    private PgDriver() {
    }


    @Override
    public boolean acceptsUrl(String url) {
        return PgDatabaseSessionFactory.acceptsUrl(url);
    }

    @Override
    public DatabaseSessionFactory createSessionFactory(String url, Map<String, Object> properties) throws JdbdException {
        return null;
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties) throws JdbdException {
        return null;
    }

    @Override
    public String productName() {
        return null;
    }

    @Override
    public DriverVersion version() {
        return null;
    }

    @Override
    public String vendor() {
        return null;
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
