package io.jdbd.mysql.protocol.conf;

import io.jdbd.vendor.conf.AbstractHostInfo;
import io.jdbd.vendor.conf.JdbcUrlParser;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;


public final class MySQLHost extends AbstractHostInfo<PropertyKey> {

    static MySQLHost create(JdbcUrlParser parser, int index) {
        return new MySQLHost(parser, index);
    }


    private final Map<PropertyKey, Object> cacheMap;

    private MySQLHost(JdbcUrlParser parser, int index) {
        super(parser, index);
        this.cacheMap = createCacheMap();
    }


    public int maxAllowedPacket() {
        return (Integer) this.cacheMap.get(PropertyKey.maxAllowedPacket);
    }


    private Map<PropertyKey, Object> createCacheMap() {

        final Map<PropertyKey, Object> map = new EnumMap<>(PropertyKey.class);
        map.put(PropertyKey.maxAllowedPacket, parseMaxAllowedPacket());

        parseMaxAllowedPacket();
        return Collections.unmodifiableMap(map);
    }


    /**
     * @see PropertyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private int parseMaxAllowedPacket() {
        int maxAllowedPacket = this.properties.getOrDefault(PropertyKey.maxAllowedPacket, Integer.class);
        final int defaultValue = 1 << 26, maxAllowedValue = 1 << 30;
        final int value;
        if (maxAllowedPacket < 0 || maxAllowedPacket == defaultValue) {
            // (1 << 26) is default value
            value = defaultValue;
        } else if (maxAllowedPacket > maxAllowedValue) {
            value = maxAllowedValue;
        } else {
            value = maxAllowedPacket & (~1023);
        }
        return value;
    }


}
