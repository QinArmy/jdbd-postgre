package io.jdbd.vendor.conf;

import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStringUtils;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractJdbcUrl<K extends IPropertyKey, H extends HostInfo<K>> implements JdbcUrl<K, H> {

    private final String originalUrl;

    private final String protocol;

    private final String subProtocol;

    private final String dbName;

    private final List<H> hostInfoList;

    protected AbstractJdbcUrl(final JdbcUrlParser parser) {

        this.originalUrl = parser.getOriginalUrl();
        this.protocol = parser.getProtocol();
        this.subProtocol = parser.getSubProtocol();
        if (!JdbdStringUtils.hasText(this.originalUrl) || !JdbdStringUtils.hasText(this.protocol)) {
            throw new IllegalArgumentException("originalUrl or protocol  is empty.");
        }
        this.dbName = parser.getGlobalProperties().get(HostInfo.DB_NAME);
        this.hostInfoList = JdbdCollections.unmodifiableList(createHostInfoList(parser));
        if (this.hostInfoList.isEmpty()) {
            throw new IllegalArgumentException("hostInfoList can't is empty.");
        }
    }

    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder("{")
                .append(internalToString())
                .append("\noriginalUrl:")
                .append(this.originalUrl)
                .append("\nprotocol:")
                .append(this.protocol)
                .append("\nsubProtocol:")
                .append(this.subProtocol)
                .append("\ndbName:")
                .append(this.dbName)
                .append("\nhostInfoList:[");
        int count = 0;
        for (HostInfo<K> hostInfo : this.hostInfoList) {
            if (count > 0) {
                builder.append(",\n");
            }
            builder.append(hostInfo);
            count++;
        }
        builder.append("]\n}");
        return builder.toString();
    }

    protected String internalToString() {
        return "";
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
    public final H getPrimaryHost() {
        return this.hostInfoList.get(0);
    }

    @Override
    public final List<H> getHostList() {
        return this.hostInfoList;
    }


    protected abstract H createHostInfo(JdbcUrlParser parser, int index);


    private List<H> createHostInfoList(final JdbcUrlParser parser) {

        List<Map<String, String>> hostMapList = parser.getHostInfo();

        final int hostSize = hostMapList.size();
        List<H> hostInfoList = new ArrayList<>(hostSize);
        for (int i = 0; i < hostSize; i++) {
            hostInfoList.add(createHostInfo(parser, i));
        }
        return hostInfoList;
    }

}
