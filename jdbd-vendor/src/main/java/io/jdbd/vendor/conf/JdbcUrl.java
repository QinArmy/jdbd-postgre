package io.jdbd.vendor.conf;

import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * @see <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html">Specifying Database Connection URLs</a>
 */
public interface JdbcUrl<K extends IPropertyKey> {

    String getOriginalUrl();

    String getProtocol();

    @Nullable
    String getDbName();

    @Nullable
    String getSubProtocol();

    HostInfo<K> getPrimaryHost();

    /**
     * @return a unmodifiable list
     */
    List<HostInfo<K>> getHostList();

}