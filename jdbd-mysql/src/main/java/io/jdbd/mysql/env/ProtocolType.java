package io.jdbd.mysql.env;

import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.conf.MyKey;
import reactor.util.annotation.Nullable;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrl.Type}
 * </p>
 */
public enum ProtocolType {

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
    private final ProtocolType alternateDnsSrvType;

    ProtocolType(String scheme, HostsCardinality cardinality) {
        this(scheme, cardinality, null, null);
    }

    ProtocolType(String scheme, HostsCardinality cardinality, @Nullable MyKey dnsSrvPropertyKey,
                 @Nullable ProtocolType alternateDnsSrvType) {
        this.scheme = scheme;
        this.cardinality = cardinality;
        this.dnsSrvPropertyKey = dnsSrvPropertyKey;
        this.alternateDnsSrvType = alternateDnsSrvType;
    }


    public final String getScheme() {
        return this.scheme;
    }

    public final HostsCardinality getCardinality() {
        return this.cardinality;
    }

    @Nullable
    public final MyKey getDnsSrvPropertyKey() {
        return this.dnsSrvPropertyKey;
    }

    @Nullable
    public final ProtocolType getAlternateDnsSrvType() {
        return this.alternateDnsSrvType;
    }


    /**
     * Returns the {@link ProtocolType} corresponding to the given scheme and number of hosts, if any.
     * Otherwise throws an {@link JdbdException}.
     * Calling this method with the argument n lower than 0 skips the hosts cardinality validation.
     *
     * @param scheme one of supported schemes
     * @param n      the number of hosts in the database URL
     * @return the {@link ProtocolType} corresponding to the given protocol and number of hosts
     */
    public static ProtocolType fromValue(String url, String scheme, int n) {
        for (ProtocolType t : ProtocolType.values()) {
            if (t.getScheme().equalsIgnoreCase(scheme) && (n < 0 || t.getCardinality().assertSize(n))) {
                return t;
            }
        }
        String m = String.format("unsupported scheme[%s] and hosts cardinality[%s] in url[%s]", scheme, n, url);
        throw new JdbdException(m);
    }

    /**
     * Checks if the given scheme corresponds to one of the connection types the driver supports.
     *
     * @param scheme scheme part from connection string, like "jdbc:mysql:"
     * @return true if the given scheme is supported by driver
     */
    public static boolean isSupported(String scheme) {
        boolean support = false;
        for (ProtocolType t : values()) {
            if (t.getScheme().equalsIgnoreCase(scheme)) {
                support = true;
                break;
            }
        }
        return support;
    }


}
