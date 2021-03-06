package io.jdbd.vendor.conf;

import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStringUtils;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultJdbcUrl<K extends IPropertyKey> implements JdbcUrl<K> {

    private static <K extends IPropertyKey> List<HostInfo<K>> createHostInfo(final JdbcUrlParser parser) {
        Map<String, String> globalMap = parser.getGlobalProperties();
        List<Map<String, String>> hostMapList = parser.getHostInfo();
        final String url = parser.getOriginalUrl();

        List<HostInfo<K>> hostInfoList = new ArrayList<>(hostMapList.size());
        for (Map<String, String> hostMap : hostMapList) {
            HostInfo<K> hostInfo = DefaultHostInfo.create(url, globalMap, hostMap);
            hostInfoList.add(hostInfo);
        }
        return hostInfoList;
    }

    private final String originalUrl;

    private final String protocol;

    private final String subProtocol;

    private final String dbName;

    private final List<HostInfo<K>> hostInfoList;

    protected DefaultJdbcUrl(final JdbcUrlParser parser) {

        this.originalUrl = parser.getOriginalUrl();
        this.protocol = parser.getProtocol();
        this.subProtocol = parser.getSubProtocol();
        if (!JdbdStringUtils.hasText(this.originalUrl) || !JdbdStringUtils.hasText(this.protocol)) {
            throw new IllegalArgumentException("originalUrl or protocol  is empty.");
        }
        this.dbName = parser.getGlobalProperties().get(HostInfo.DB_NAME);
        this.hostInfoList = JdbdCollections.unmodifiableList(createHostInfo(parser));
    }

    @Override
    public final String getOriginalUrl() {
        return this.originalUrl;
    }

    @Override
    public final String getProtocol() {
        return this.protocol;
    }

    @Override
    public final String getDbName() {
        return this.dbName;
    }

    @Nullable
    @Override
    public final String getSubProtocol() {
        return this.subProtocol;
    }

    @Override
    public final HostInfo<K> getPrimaryHost() {
        return this.hostInfoList.get(0);
    }

    @Override
    public final List<HostInfo<K>> getHostList() {
        return this.hostInfoList;
    }


}
