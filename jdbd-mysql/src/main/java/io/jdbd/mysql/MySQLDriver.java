package io.jdbd.mysql;

import io.jdbd.Driver;
import io.jdbd.config.PropertyException;
import io.jdbd.config.UrlException;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.MySQLDatabaseSessionFactory;
import io.jdbd.session.DatabaseSessionFactory;
import reactor.util.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

public final class MySQLDriver implements Driver {

    private static final MySQLDriver INSTANCE = new MySQLDriver();

    public static MySQLDriver getInstance() {
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
    public DatabaseSessionFactory createSessionFactory(String url, Map<String, String> properties)
            throws UrlException, PropertyException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties)
            throws UrlException, PropertyException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.forPoolVendor(url, properties);
    }


    public static int getMajorVersion() {
        final String version = MySQLDriver.class.getPackage().getImplementationVersion();
        try {
            return Integer.parseInt(version.substring(0, getMajorVersionIndex(version)));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    public static int getMinorVersion() {
        final String version = MySQLDriver.class.getPackage().getImplementationVersion();
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
        return MySQLDriver.class.getName();
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
