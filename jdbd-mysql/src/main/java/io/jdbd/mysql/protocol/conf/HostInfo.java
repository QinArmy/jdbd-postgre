package io.jdbd.mysql.protocol.conf;


import io.jdbd.mysql.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;

public final class HostInfo {

    private static final String HOST_PORT_SEPARATOR = ":";

    private final String originalUrl;
    private final String host;
    private final int port;
    private final String user;

    private final String password;
    private final boolean isPasswordLess;
    private final Properties properties;

    HostInfo(String originalUrl, String user, @Nullable String password) {
        this(originalUrl, MySQLUrl.DEFAULT_HOST, MySQLUrl.DEFAULT_PORT, user, password, Collections.emptyMap());
    }

    HostInfo(String originalUrl, String host, int port
            , String user, @Nullable String password
            , Map<String, String> hostProperties) {

        if (!StringUtils.hasText(user)) {
            throw new IllegalArgumentException("user error");
        }
        this.originalUrl = originalUrl;
        this.host = host;
        this.port = port;
        this.user = user;

        this.password = password;
        this.isPasswordLess = password == null;
        this.properties = ImmutableMapProperties.getInstance(hostProperties);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HostInfo.class.getSimpleName() + "[", "]")
                .add("originalUrl='" + originalUrl + "'")
                .add("host='" + host + "'")
                .add("port=" + port)
                // .add("user='" + user + "'")
                // .add("password='" + password + "'")
                .add("isPasswordLess=" + isPasswordLess)
                .add("properties=" + properties)
                .toString();
    }

    public String getHostPortPair() {
        return this.host + HOST_PORT_SEPARATOR + this.port;
    }

    public String getOriginalUrl() {
        return this.originalUrl;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String getUser() {
        return this.user;
    }

    public String getPassword() {
        return this.password;
    }

    public boolean isPasswordLess() {
        return this.isPasswordLess;
    }

    public Properties getProperties() {
        return this.properties;
    }
}
