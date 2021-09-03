package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.AbstractJdbcUrl;
import io.jdbd.vendor.conf.JdbcUrlParser;

import java.util.Map;

public final class PostgreUrl extends AbstractJdbcUrl<PgKey, PostgreHost> {

    public static PostgreUrl create(String url, Map<String, String> propMap) {
        return new PostgreUrl(PostgreUrlParser.create(url, propMap));
    }

    public static boolean acceptsUrl(final String url) {
        return url.startsWith(PROTOCOL);
    }

    static final String PROTOCOL = "jdbc:postgresql:";

    private PostgreUrl(PostgreUrlParser parser) {
        super(parser);
    }


    @Override
    protected final PostgreHost createHostInfo(JdbcUrlParser parser, int index) {
        return PostgreHost.create(parser, index);
    }

    @Override
    protected final PgKey getDbNameKey() {
        return PgKey.PGDBNAME;
    }

    public <T> T getOrDefault(PgKey key, Class<T> targetType) {
        // TODO complete me
        T value;
        if (key == PgKey.factoryWorkerCount && targetType == Integer.class) {
            value = targetType.cast(20);
        } else {
            throw new IllegalArgumentException("TOTO not complete");
        }
        return value;
    }


}
