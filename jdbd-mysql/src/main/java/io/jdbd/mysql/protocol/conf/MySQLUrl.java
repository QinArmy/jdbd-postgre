package io.jdbd.mysql.protocol.conf;


import io.jdbd.PropertyException;
import io.jdbd.UrlException;
import io.jdbd.mysql.protocol.client.Charsets;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.vendor.conf.*;
import io.qinarmy.env.convert.ConverterManager;
import io.qinarmy.env.convert.ImmutableConverterManager;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrl}
 * </p>
 */
public final class MySQLUrl extends AbstractJdbcUrl {

    public static final int DEFAULT_PORT = 3306;

    public static MySQLUrl getInstance(final String url, final Map<String, String> properties) {
        final MySQLUrl mySQLUrl;
        mySQLUrl = new MySQLUrl(MySQLUrlParser.parseMySQLUrl(url, properties));
        checkUrlProperties(mySQLUrl.getCommonProps());
        for (MySQLHost host : mySQLUrl.hostList) {
            Properties props = host.getProperties();
            checkUrlProperties(props);
            checkKeyProperties(props);
        }


        return mySQLUrl;
    }

    public static boolean acceptsUrl(final String url) {
        boolean accept = false;
        for (Protocol protocol : Protocol.values()) {
            if (url.startsWith(protocol.scheme)) {
                accept = true;
                break;
            }
        }
        return accept;
    }

    public final Protocol protocolType;

    private final List<MySQLHost> hostList;

    private final int maxAllowedPayload;

    private MySQLUrl(MySQLUrlParser parser) {
        super(parser);
        this.hostList = createHostInfoList(parser);
        this.protocolType = Protocol.fromValue(this.getOriginalUrl(), this.getProtocol(), this.getHostList().size());
        this.maxAllowedPayload = parseMaxAllowedPacket();
    }

    @Override
    public MySQLHost getPrimaryHost() {
        return this.hostList.get(0);
    }

    @Override
    public List<MySQLHost> getHostList() {
        return this.hostList;
    }

    public int getMaxAllowedPayload() {
        return this.maxAllowedPayload;
    }

    @Override
    protected String internalToString() {
        return this.protocolType.name();
    }


    @Override
    protected PropertyKey getDbNameKey() {
        return MyKey.dbname;
    }

    @Override
    protected Properties createProperties(Map<String, String> map) {
        return wrapProperties(map);
    }


    /**
     * @see MyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private int parseMaxAllowedPacket() {
        final int maxAllowedPacket = this.commonProps.getOrDefault(MyKey.maxAllowedPacket, Integer.class);
        final int defaultValue = 1 << 26, minValue = 1024;
        final int value;
        if (maxAllowedPacket < 0 || maxAllowedPacket == defaultValue) {
            // (1 << 26) is default value
            value = defaultValue;
        } else if (maxAllowedPacket < minValue) {
            value = minValue;
        } else if (maxAllowedPacket > ClientProtocol.MAX_PAYLOAD_SIZE) {
            value = ClientProtocol.MAX_PAYLOAD_SIZE;
        } else {
            value = maxAllowedPacket & (~1023);
        }
        return value;
    }

    public static Properties wrapProperties(Map<String, String> map) {
        ConverterManager converterManager = ImmutableConverterManager.create(Converters::registerConverter);
        return ImmutableMapProperties.getInstance(map, converterManager);
    }

    private static List<MySQLHost> createHostInfoList(JdbcUrlParser parser) {
        final List<Map<String, String>> hostMapList = parser.getHostInfo();

        final int hostSize = hostMapList.size();
        List<MySQLHost> hostInfoList = new ArrayList<>(hostSize);
        for (int i = 0; i < hostSize; i++) {
            hostInfoList.add(MySQLHost.create(parser, i));
        }
        return hostInfoList;
    }

    private static void checkUrlProperties(final Properties properties) {
        MyKey currentKey = null;
        try {
            for (MyKey key : MyKey.values()) {
                currentKey = key;
                properties.get(key, key.getJavaType());
            }
        } catch (PropertyException e) {
            throw e;
        } catch (Throwable e) {
            final String m, propName;
            if (currentKey == null) {
                // never here
                propName = "unknown property";
                m = "Property error";
            } else {
                propName = currentKey.getKey();
                m = String.format("Property[%s] format error.", propName);
            }
            throw new PropertyException(propName, m, e);
        }
    }

    private static void checkKeyProperties(final Properties properties) {
        if (properties.getOrDefault(MyKey.factoryWorkerCount, Integer.class) < 1) {
            throw errorPropertyValue(MyKey.factoryWorkerCount);
        }
        final Charset charset;
        charset = properties.get(MyKey.characterEncoding, Charset.class);
        if (charset != null && Charsets.isUnsupportedCharsetClient(charset.name())) {
            String m = String.format(
                    "Property[%s] value[%s] isn unsupported client charset,because encode US_ASCII as multi bytes."
                    , MyKey.characterEncoding, charset.name());
            throw new PropertyException(MyKey.characterEncoding.getKey(), m);
        }


    }

    private static PropertyException errorPropertyValue(final MyKey key) {
        String m = String.format("Property[%s] value error.", key);
        return new PropertyException(key.getKey(), m);
    }


    /**
     * The rules describing the number of hosts a database URL may contain.
     */
    public enum HostsCardinality {

        SINGLE {
            @Override
            public boolean assertSize(int n) {
                return n == 1;
            }
        },
        MULTIPLE {
            @Override
            public boolean assertSize(int n) {
                return n > 1;
            }
        },
        ONE_OR_MORE {
            @Override
            public boolean assertSize(int n) {
                return n >= 1;
            }
        };

        public abstract boolean assertSize(int n);
    }


    /**
     * <p>
     * {@code com.mysql.cj.conf.ConnectionUrl.Type}
     * </p>
     */
    public enum Protocol {

        // DNS SRV schemes (cardinality is validated by implementing classes):
        FAILOVER_DNS_SRV_CONNECTION("jdbc:mysql+srv:", HostsCardinality.ONE_OR_MORE), //
        LOADBALANCE_DNS_SRV_CONNECTION("jdbc:mysql+srv:loadbalance:", HostsCardinality.ONE_OR_MORE), //
        REPLICATION_DNS_SRV_CONNECTION("jdbc:mysql+srv:replication:", HostsCardinality.ONE_OR_MORE), //
        // Standard schemes:
        SINGLE_CONNECTION("jdbc:mysql:", HostsCardinality.SINGLE, MyKey.dnsSrv, FAILOVER_DNS_SRV_CONNECTION), //
        FAILOVER_CONNECTION("jdbc:mysql:", HostsCardinality.MULTIPLE, MyKey.dnsSrv,
                FAILOVER_DNS_SRV_CONNECTION), //
        LOADBALANCE_CONNECTION("jdbc:mysql:loadbalance:", HostsCardinality.ONE_OR_MORE, MyKey.dnsSrv,
                LOADBALANCE_DNS_SRV_CONNECTION), //
        REPLICATION_CONNECTION("jdbc:mysql:replication:", HostsCardinality.ONE_OR_MORE, MyKey.dnsSrv,
                REPLICATION_DNS_SRV_CONNECTION); //

        private final String scheme;
        private final HostsCardinality cardinality;
        private final MyKey dnsSrvPropertyKey;
        private final Protocol alternateDnsSrvType;

        Protocol(String scheme, HostsCardinality cardinality) {
            this(scheme, cardinality, null, null);
        }

        Protocol(String scheme, HostsCardinality cardinality
                , @Nullable MyKey dnsSrvPropertyKey, @Nullable Protocol alternateDnsSrvType) {
            this.scheme = scheme;
            this.cardinality = cardinality;
            this.dnsSrvPropertyKey = dnsSrvPropertyKey;
            this.alternateDnsSrvType = alternateDnsSrvType;
        }


        public String getScheme() {
            return this.scheme;
        }

        public HostsCardinality getCardinality() {
            return this.cardinality;
        }

        @Nullable
        public MyKey getDnsSrvPropertyKey() {
            return this.dnsSrvPropertyKey;
        }

        @Nullable
        public Protocol getAlternateDnsSrvType() {
            return this.alternateDnsSrvType;
        }


        /**
         * Returns the {@link Protocol} corresponding to the given scheme and number of hosts, if any.
         * Otherwise throws an {@link UrlException}.
         * Calling this method with the argument n lower than 0 skips the hosts cardinality validation.
         *
         * @param scheme one of supported schemes
         * @param n      the number of hosts in the database URL
         * @return the {@link Protocol} corresponding to the given protocol and number of hosts
         */
        public static Protocol fromValue(String url, String scheme, int n) {
            for (Protocol t : Protocol.values()) {
                if (t.getScheme().equalsIgnoreCase(scheme) && (n < 0 || t.getCardinality().assertSize(n))) {
                    return t;
                }
            }
            String message = String.format("unsupported scheme[%s] and hosts cardinality[%s]", scheme, n);
            throw new UrlException(url, message);
        }

        /**
         * Checks if the given scheme corresponds to one of the connection types the driver supports.
         *
         * @param scheme scheme part from connection string, like "jdbc:mysql:"
         * @return true if the given scheme is supported by driver
         */
        public static boolean isSupported(String scheme) {
            for (Protocol t : values()) {
                if (t.getScheme().equalsIgnoreCase(scheme)) {
                    return true;
                }
            }
            return false;
        }
    }


}
