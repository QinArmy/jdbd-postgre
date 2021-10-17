package io.jdbd.vendor.conf;

import io.jdbd.vendor.util.JdbdStrings;
import reactor.util.annotation.Nullable;

import java.util.Map;

public abstract class AbstractJdbcUrl implements JdbcUrl {

    private final String originalUrl;

    private final String protocol;

    private final String subProtocol;

    private final String dbName;

    protected final Properties commonProps;


    protected AbstractJdbcUrl(final JdbcUrlParser parser) {

        this.originalUrl = parser.getOriginalUrl();
        this.protocol = parser.getProtocol();
        this.subProtocol = parser.getSubProtocol();
        if (!JdbdStrings.hasText(this.originalUrl) || !JdbdStrings.hasText(this.protocol)) {
            throw new IllegalArgumentException("originalUrl or protocol  is empty.");
        }
        this.dbName = getValue(parser.getGlobalProperties(), getDbNameKey());
        this.commonProps = createProperties(parser.getGlobalProperties());
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
        for (HostInfo hostInfo : getHostList()) {
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

    @Nullable
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
    public final Properties getCommonProps() {
        return this.commonProps;
    }

    protected abstract PropertyKey getDbNameKey();

    protected abstract Properties createProperties(Map<String, String> map);


    @Nullable
    protected static String getValue(Map<String, String> map, PropertyKey propertyKey) {
        String keyName = propertyKey.getKey();
        String value = map.get(keyName);
        if (value == null && !propertyKey.isCaseSensitive()) {
            value = map.get(keyName.toLowerCase());
            if (value == null) {
                value = map.get(keyName.toUpperCase());
            }
        }
        return value;
    }

}
