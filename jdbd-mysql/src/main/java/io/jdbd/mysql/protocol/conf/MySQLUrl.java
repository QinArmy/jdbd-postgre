package io.jdbd.mysql.protocol.conf;


import io.jdbd.config.UrlException;
import io.jdbd.vendor.conf.AbstractJdbcUrl;
import io.jdbd.vendor.conf.JdbcUrlParser;
import io.jdbd.vendor.conf.PropertyKey;
import reactor.util.annotation.Nullable;

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

    public static MySQLUrl getInstance(String url, Map<String, String> properties) {
        return new MySQLUrl(MySQLUrlParser.parseMySQLUrl(url, properties));
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

    private MySQLUrl(MySQLUrlParser parser) {
        super(parser);
        this.hostList = createHostInfoList(parser);
        this.protocolType = Protocol.fromValue(this.getOriginalUrl(), this.getProtocol(), this.getHostList().size());
    }

    @Override
    public MySQLHost getPrimaryHost() {
        return this.hostList.get(0);
    }

    @Override
    public List<MySQLHost> getHostList() {
        return this.hostList;
    }

    @Override
    protected String internalToString() {
        return this.protocolType.name();
    }


    @Override
    protected PropertyKey getDbNameKey() {
        return MyKey.dbname;
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
