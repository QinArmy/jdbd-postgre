package io.jdbd.postgre;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.postgre.env.PgUrlParser;
import io.jdbd.postgre.session.PgDatabaseSessionFactory;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.util.DefaultDriverVersion;

import java.util.Map;

public final class PgDriver implements Driver {

    private static final PgDriver INSTANCE = new PgDriver();

    public static PgDriver getInstance() {
        return INSTANCE;
    }

    public static final String POSTGRE_SQL = "PostgreSQL";

    public static final String PG_DRIVER_VENDOR = "io.jdbd.postgre";

    private final DriverVersion version;

    private PgDriver() {
        version = DefaultDriverVersion.from(PgDriver.class.getName(), PgDriver.class);
    }


    @Override
    public boolean acceptsUrl(final @Nullable String url) {
        return url != null && url.startsWith(PgUrlParser.PROTOCOL);
    }

    @Override
    public DatabaseSessionFactory createSessionFactory(String url, Map<String, Object> properties) throws JdbdException {
        return PgDatabaseSessionFactory.create(url, properties, false);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties) throws JdbdException {
        return PgDatabaseSessionFactory.create(url, properties, true);
    }

    @Override
    public String productName() {
        return POSTGRE_SQL;
    }

    @Override
    public DriverVersion version() {
        return this.version;
    }

    @Override
    public String vendor() {
        return PG_DRIVER_VENDOR;
    }

    @Override
    public String toString() {
        return PgStrings.builder()
                .append(getClass().getName())
                .append("[ vendor : ")
                .append(vendor())
                .append(" , productName : ")
                .append(productName())
                .append(" , version : ")
                .append(version())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
