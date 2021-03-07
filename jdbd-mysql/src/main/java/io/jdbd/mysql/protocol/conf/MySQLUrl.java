package io.jdbd.mysql.protocol.conf;


import io.jdbd.UrlException;
import io.jdbd.vendor.conf.DefaultJdbcUrl;
import reactor.util.annotation.Nullable;

import java.util.Map;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrl}
 * </p>
 */
public final class MySQLUrl extends DefaultJdbcUrl<PropertyKey> {

    public static final int DEFAULT_PORT = 3306;

    public static MySQLUrl getInstance(String url, Map<String, String> properties) {
        return new MySQLUrl(MySQLUrlParser.parseMySQLUrl(url, properties));
    }

    private final Protocol protocolType;


    private MySQLUrl(MySQLUrlParser parser) {
        super(parser);
        this.protocolType = Protocol.fromValue(this.getOriginalUrl(), this.getProtocol(), this.getHostList().size());
    }

    public Protocol getProtocolType() {
        return protocolType;
    }

    @Override
    protected String internalToString() {
        return this.protocolType.name();
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
        XDEVAPI_DNS_SRV_SESSION("mysqlx+srv:", HostsCardinality.ONE_OR_MORE), //
        // Standard schemes:
        SINGLE_CONNECTION("jdbc:mysql:", HostsCardinality.SINGLE, PropertyKey.dnsSrv, FAILOVER_DNS_SRV_CONNECTION), //
        FAILOVER_CONNECTION("jdbc:mysql:", HostsCardinality.MULTIPLE, PropertyKey.dnsSrv,
                FAILOVER_DNS_SRV_CONNECTION), //
        LOADBALANCE_CONNECTION("jdbc:mysql:loadbalance:", HostsCardinality.ONE_OR_MORE, PropertyKey.dnsSrv,
                LOADBALANCE_DNS_SRV_CONNECTION), //
        REPLICATION_CONNECTION("jdbc:mysql:replication:", HostsCardinality.ONE_OR_MORE, PropertyKey.dnsSrv,
                REPLICATION_DNS_SRV_CONNECTION), //
        XDEVAPI_SESSION("mysqlx:", HostsCardinality.ONE_OR_MORE, PropertyKey.xdevapiDnsSrv,
                XDEVAPI_DNS_SRV_SESSION);

        private final String scheme;
        private final HostsCardinality cardinality;
        private final PropertyKey dnsSrvPropertyKey;
        private final Protocol alternateDnsSrvType;

        Protocol(String scheme, HostsCardinality cardinality) {
            this(scheme, cardinality, null, null);
        }

        Protocol(String scheme, HostsCardinality cardinality
                , @Nullable PropertyKey dnsSrvPropertyKey, @Nullable Protocol alternateDnsSrvType) {
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
        public PropertyKey getDnsSrvPropertyKey() {
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
            throw new UrlException(url, "unsupported scheme[%s] and hosts cardinality[%s]", scheme, n);
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
