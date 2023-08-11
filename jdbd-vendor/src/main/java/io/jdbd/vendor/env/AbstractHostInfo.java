package io.jdbd.vendor.env;


import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStrings;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Deprecated
public abstract class AbstractHostInfo implements HostInfo {


    protected final String originalUrl;
    protected final String host;
    protected final int port;
    protected final String user;

    protected final String password;
    protected final boolean isPasswordLess;
    protected final Properties properties;

    protected final String dbName;


    protected AbstractHostInfo(JdbcUrlParser parser, int index) {
        this.originalUrl = Objects.requireNonNull(parser.getOriginalUrl(), "getOriginalUrl");
        final Map<String, String> globalProperties = parser.getGlobalProperties();
        final Map<String, String> hostProperties = parser.getHostInfo().get(index);

        if (!JdbdStrings.hasText(this.originalUrl)
                || hostProperties == null
                || JdbdCollections.isEmpty(globalProperties)) {
            throw new IllegalArgumentException("please check arguments.");
        }
        int capacity = (int) ((globalProperties.size() + hostProperties.size()) / 0.75F);
        final Map<String, String> map = new HashMap<>(capacity);
        //firstly
        map.putAll(globalProperties);
        // secondly
        map.putAll(hostProperties);


        final String host = removeValue(map, getHostKey());

        this.host = JdbdStrings.hasText(host) ? host : DEFAULT_HOST;
        final String portText = removeValue(map, getPortKey());
        if (portText == null) {
            this.port = getDefaultPort();
        } else {
            try {
                this.port = Integer.parseInt(portText);
            } catch (NumberFormatException e) {
                String message = String.format("post[%s] format error", portText);
                throw new UrlException(this.originalUrl, message, e);
            }
        }

        this.user = removeValue(map, getUserKey());
        this.password = removeValue(map, getPasswordKey());

        if (!JdbdStrings.hasText(this.user)) {
            String message = String.format("%s property must be not empty", getUserKey().name());
            throw new UrlException(this.originalUrl, message);
        }
        this.isPasswordLess = !JdbdStrings.hasText(this.password);
        this.dbName = removeValue(map, getDbNameKey());

        this.properties = createProperties(map);
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
    public Properties getProperties() {
        return this.properties;
    }

    @Nullable
    public String getDbName() {
        return this.dbName;
    }


    protected Properties createProperties(Map<String, String> map) {
        return ImmutableMapProperties.getInstance(map);
    }

    protected abstract PropertyKey getUserKey();

    protected abstract PropertyKey getPasswordKey();

    protected abstract PropertyKey getHostKey();

    protected abstract PropertyKey getPortKey();

    protected abstract PropertyKey getDbNameKey();


    protected abstract int getDefaultPort();


    @Nullable
    protected static String removeValue(Map<String, String> map, PropertyKey propertyKey) {
        String keyName = propertyKey.getKey();
        String value = map.remove(keyName);
        if (value == null && !propertyKey.isCaseSensitive()) {
            value = map.remove(keyName.toLowerCase());
            if (value == null) {
                value = map.remove(keyName.toUpperCase());
            }
        }
        return value;
    }


}
