package io.jdbd.vendor.env;

import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;

@Deprecated
public interface JdbcUrlParser {

    String getOriginalUrl();

    String getProtocol();

    @Nullable
    String getDbName();

    @Nullable
    String getSubProtocol();

    /**
     * @return a unmodifiable map
     */
    Map<String, Object> getGlobalProperties();

    /**
     * @return a unmodifiable list
     */
    List<Map<String, Object>> getHostInfo();

}
