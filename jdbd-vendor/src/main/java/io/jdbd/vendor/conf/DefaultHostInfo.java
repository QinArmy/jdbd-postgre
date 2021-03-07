package io.jdbd.vendor.conf;


import io.jdbd.UrlException;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStringUtils;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

public final class DefaultHostInfo<K extends IPropertyKey> implements HostInfo<K> {


    public static <K extends IPropertyKey> DefaultHostInfo<K> create(final String originalUrl
            , final Map<String, String> globalProperties
            , final Map<String, String> hostProperties) {
        return new DefaultHostInfo<>(originalUrl, globalProperties, hostProperties);
    }


    private final String originalUrl;
    private final String host;
    private final int port;
    private final String user;

    private final String password;
    private final boolean isPasswordLess;
    private final Properties<K> properties;

    private final String dbName;


    protected DefaultHostInfo(final String originalUrl, final Map<String, String> globalProperties
            , final Map<String, String> hostProperties) {
        if (!JdbdStringUtils.hasText(originalUrl)
                || JdbdCollections.isEmpty(hostProperties)
                || JdbdCollections.isEmpty(globalProperties)) {
            throw new IllegalArgumentException("please check arguments.");
        }
        int capacity = (int) ((globalProperties.size() + hostProperties.size()) / 0.75F);
        final Map<String, String> map = new HashMap<>(capacity);
        //firstly
        map.putAll(globalProperties);
        // secondly
        map.putAll(hostProperties);

        this.originalUrl = originalUrl;
        String host = map.remove(HOST);

        this.host = JdbdStringUtils.hasText(host) ? host : DEFAULT_HOST;
        final String portText = map.remove(PORT);
        try {
            this.port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new UrlException(e, this.originalUrl, "post[%s] format error", portText);
        }
        this.user = map.remove(USER);
        this.password = map.remove(PASSWORD);

        if (!JdbdStringUtils.hasText(this.user)) {
            throw new UrlException(this.originalUrl, "%s property must be not empty", USER);
        }
        this.isPasswordLess = !JdbdStringUtils.hasText(this.password);
        this.dbName = map.remove(DB_NAME);

        this.properties = ImmutableMapProperties.getInstance(map);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("(")
                .append("host = '").append(this.host)
                .append(", port = ").append(this.port)
                .append(", isPasswordLess = ").append(this.isPasswordLess)
                .append(", propertiesSize = ")
                .append(this.properties.size())
                .append(')')
                .toString();
    }

    @Override
    public String getHostPortPair() {
        return this.host + HOST_PORT_SEPARATOR + this.port;
    }

    @Override
    public String getOriginalUrl() {
        return this.originalUrl;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Nullable
    public String getPassword() {
        return this.password;
    }

    @Override
    public boolean isPasswordLess() {
        return this.isPasswordLess;
    }

    @Override
    public Properties<K> getProperties() {
        return this.properties;
    }

    @Nullable
    public String getDbName() {
        return this.dbName;
    }
}
