package io.jdbd.mysql.protocol.conf;

import io.jdbd.env.AbstractHostInfo;
import io.jdbd.env.JdbcUrlParser;
import io.jdbd.env.PropertyKey;

import java.util.Collections;
import java.util.Map;


public final class MySQLHost extends AbstractHostInfo {

    static MySQLHost create(JdbcUrlParser parser, int index) {
        return new MySQLHost(parser, index);
    }


    public static final int MAX_ALLOWED_PAYLOAD = 1 << 30;


    private final Map<MyKey, Object> cacheMap;

    private final int maxAllowedPayload;

    private final boolean clientPrepareSupportStream;

    private MySQLHost(JdbcUrlParser parser, int index) {
        super(parser, index);
        this.cacheMap = createCacheMap();
        this.maxAllowedPayload = parseMaxAllowedPacket();
        this.clientPrepareSupportStream = this.maxAllowedPayload == MAX_ALLOWED_PAYLOAD
                && this.properties.getOrDefault(MyKey.clientPrepareSupportStream, Boolean.class);
    }


    /**
     * <p>
     *     <ul>
     *         <li>The client can send up to as many bytes as {@link #maxAllowedPayload} value.However, the server does not receive from the client more bytes than the current global max_allowed_packet value.</li>
     *         <li>The client can receive up to as many bytes as the max_allowed_packet session value. However, the server does not send to the client more bytes than the current global max_allowed_packet value. </li>
     *     </ul>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    public final int maxAllowedPayload() {
        return this.maxAllowedPayload;
    }


    public boolean clientPrepareSupportStream() {
        return this.clientPrepareSupportStream;
    }


    @Override
    protected PropertyKey getUserKey() {
        return MyKey.user;
    }

    @Override
    protected PropertyKey getPasswordKey() {
        return MyKey.password;
    }

    @Override
    protected PropertyKey getHostKey() {
        return MyKey.host;
    }

    @Override
    protected PropertyKey getPortKey() {
        return MyKey.port;
    }

    @Override
    protected PropertyKey getDbNameKey() {
        return MyKey.dbname;
    }

    @Override
    protected int getDefaultPort() {
        return MySQLUrl.DEFAULT_PORT;
    }

    private Map<MyKey, Object> createCacheMap() {

//        final Map<PropertyKey, Object> map = new EnumMap<>(PropertyKey.class);
//        map.put(PropertyKey.maxAllowedPacket, parseMaxAllowedPacket());
//
//        parseMaxAllowedPacket();
//        return Collections.unmodifiableMap(map);
        return Collections.emptyMap();
    }


    /**
     * @see MyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private int parseMaxAllowedPacket() {
        final int maxAllowedPacket = this.properties.getOrDefault(MyKey.maxAllowedPacket, Integer.class);
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
