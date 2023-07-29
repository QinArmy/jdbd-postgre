package io.jdbd.vendor.env;

import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * @see <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html">Specifying Database Connection URLs</a>
 */
@Deprecated
public interface JdbcUrl {

    String getOriginalUrl();

    String getProtocol();

    @Nullable
    String getDbName();

    @Nullable
    String getSubProtocol();

    HostInfo getPrimaryHost();

    /**
     * @return a unmodifiable list
     */
    List<? extends HostInfo> getHostList();

    Properties getCommonProps();

}
