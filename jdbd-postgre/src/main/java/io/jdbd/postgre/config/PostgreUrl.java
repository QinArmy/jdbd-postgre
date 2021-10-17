package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.AbstractJdbcUrl;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.JdbcUrlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class PostgreUrl extends AbstractJdbcUrl {

    public static PostgreUrl create(String url, Map<String, String> propMap) {
        return new PostgreUrl(PostgreUrlParser.create(url, propMap));
    }

    public static boolean acceptsUrl(final String url) {
        return url.startsWith(PROTOCOL);
    }

    static final String PROTOCOL = "jdbc:postgresql:";

    private final List<PgHost> hostList;

    private PostgreUrl(PostgreUrlParser parser) {
        super(parser);
        this.hostList = createHostInfoList(parser);
    }


    @Override
    public HostInfo getPrimaryHost() {
        return this.hostList.get(0);
    }

    @Override
    public List<PgHost> getHostList() {
        return this.hostList;
    }

    @Override
    protected PgKey getDbNameKey() {
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

    private static List<PgHost> createHostInfoList(JdbcUrlParser parser) {
        final List<Map<String, String>> hostMapList = parser.getHostInfo();

        final int hostSize = hostMapList.size();
        List<PgHost> hostInfoList = new ArrayList<>(hostSize);
        for (int i = 0; i < hostSize; i++) {
            hostInfoList.add(PgHost.create(parser, i));
        }
        return hostInfoList;
    }


}
