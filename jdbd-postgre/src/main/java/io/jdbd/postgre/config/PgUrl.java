package io.jdbd.postgre.config;

import io.jdbd.env.*;
import io.qinarmy.env.convert.ConverterManager;
import io.qinarmy.env.convert.ImmutableConverterManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class PgUrl extends AbstractJdbcUrl {

    public static PgUrl create(String url, Map<String, String> propMap) {
        return new PgUrl(PostgreUrlParser.create(url, propMap));
    }

    public static boolean acceptsUrl(final String url) {
        return url.startsWith(PROTOCOL);
    }

    static final String PROTOCOL = "jdbc:postgresql:";

    private final List<PgHost> hostList;

    private PgUrl(PostgreUrlParser parser) {
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

    @Override
    protected Properties createProperties(Map<String, String> map) {
        return wrapProperties(map);
    }

    static Properties wrapProperties(Map<String, String> map) {
        ConverterManager converterManager = ImmutableConverterManager.create(Converters::registerConverter);
        return ImmutableMapProperties.getInstance(map, converterManager);
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
