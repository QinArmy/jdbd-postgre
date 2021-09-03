package io.jdbd.postgre;

import io.jdbd.DatabaseSessionFactory;
import io.jdbd.Driver;
import io.jdbd.JdbdNonSQLException;
import io.jdbd.postgre.session.PgDatabaseSessionFactory;
import reactor.util.annotation.Nullable;

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
    public final boolean acceptsUrl(String url) {
        return PgDatabaseSessionFactory.acceptsUrl(url);
    }

    @Override
    public final DatabaseSessionFactory createSessionFactory(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return PgDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public final DatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return PgDatabaseSessionFactory.forPoolVendor(url, properties);
    }


    public static int getMajorVersion() {
        final String version = PgDriver.class.getPackage().getSpecificationVersion();
        try {
            return Integer.parseInt(version.substring(0, getMajorVersionIndex(version)));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    public static int getMinorVersion() {
        final String version = PgDriver.class.getPackage().getSpecificationVersion();
        if (version == null) {
            // here run in jdbd-postgre project test environment.
            throw createPacketWithErrorWay();
        }
        final int majorIndex = getMajorVersionIndex(version);
        final int minorIndex = version.indexOf('-', majorIndex);
        if (minorIndex < 0) {
            throw createPacketWithErrorWay();
        }
        try {
            return Integer.parseInt(version.substring(majorIndex + 1, minorIndex));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    public static String getName() {
        return PgDriver.class.getName();
    }


    /**
     * @see #getMajorVersion()
     * @see #getMinorVersion()
     */
    private static int getMajorVersionIndex(@Nullable String version) {

        if (version == null) {
            // here run in jdbd-postgre project test environment.
            throw createPacketWithErrorWay();
        }
        final int index = version.indexOf('.');
        if (index < 0) {
            throw createPacketWithErrorWay();
        }
        return index;
    }


    private static IllegalStateException createPacketWithErrorWay() {
        return new IllegalStateException("jdbd-postgre packet with error way.");
    }


}
