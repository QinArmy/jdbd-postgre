package io.jdbd.mysql.protocol.conf;

import io.jdbd.vendor.conf.AbstractHostInfo;
import io.jdbd.vendor.conf.IPropertyKey;
import io.jdbd.vendor.conf.JdbcUrlParser;

import java.util.Collections;
import java.util.Map;


public final class MySQLHost extends AbstractHostInfo<PropertyKey> {

    static MySQLHost create(JdbcUrlParser parser, int index) {
        return new MySQLHost(parser, index);
    }


    public static final int MAX_ALLOWED_PAYLOAD = 1 << 30;


    private final Map<PropertyKey, Object> cacheMap;

    private final int maxAllowedPayload;

    private final boolean clientPrepareSupportStream;

    private MySQLHost(JdbcUrlParser parser, int index) {
        super(parser, index);
        this.cacheMap = createCacheMap();
        this.maxAllowedPayload = parseMaxAllowedPacket();
        this.clientPrepareSupportStream = this.maxAllowedPayload == MAX_ALLOWED_PAYLOAD
                && this.properties.getOrDefault(PropertyKey.clientPrepareSupportStream, Boolean.class);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    public int maxAllowedPayload() {
        return this.maxAllowedPayload;
    }

    public boolean clientPrepareSupportStream() {
        return this.clientPrepareSupportStream;
    }


    @Override
    protected IPropertyKey getUserKey() {
        return PropertyKey.user;
    }

    @Override
    protected IPropertyKey getPasswordKey() {
        return PropertyKey.password;
    }

    @Override
    protected IPropertyKey getHostKey() {
        return PropertyKey.host;
    }

    @Override
    protected IPropertyKey getPortKey() {
        return PropertyKey.port;
    }

    @Override
    protected IPropertyKey getDbNameKey() {
        return PropertyKey.dbname;
    }

    @Override
    protected int getDefaultPort() {
        return MySQLUrl.DEFAULT_PORT;
    }

    private Map<PropertyKey, Object> createCacheMap() {

//        final Map<PropertyKey, Object> map = new EnumMap<>(PropertyKey.class);
//        map.put(PropertyKey.maxAllowedPacket, parseMaxAllowedPacket());
//
//        parseMaxAllowedPacket();
//        return Collections.unmodifiableMap(map);
        return Collections.emptyMap();
    }


    /**
     * @see PropertyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private int parseMaxAllowedPacket() {
        final int maxAllowedPacket = this.properties.getOrDefault(PropertyKey.maxAllowedPacket, Integer.class);
        final int defaultValue = 1 << 26, minValue = 1024;
        final int value;
        if (maxAllowedPacket < 0 || maxAllowedPacket == defaultValue) {
            // (1 << 26) is default value
            value = defaultValue;
        } else if (maxAllowedPacket < minValue) {
            value = minValue;
        } else if (maxAllowedPacket > MAX_ALLOWED_PAYLOAD) {
            value = MAX_ALLOWED_PAYLOAD;
        } else {
            value = maxAllowedPacket & (~1023);
        }
        return value;
    }


}
