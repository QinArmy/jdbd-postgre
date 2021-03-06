package io.jdbd.vendor.conf;

import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;

public interface JdbcUrlParser {

    String getOriginalUrl();

    String getProtocol();

    @Nullable
    String getDatabase();

    @Nullable
    String getSubProtocol();

    /**
     * @return a modifiable map,see {@link DefaultJdbcUrl#create(JdbcUrlParser)}.
     */
    Map<String, String> getGlobalProperties();

    /**
     * @return a list that each element is modifiable,see {@link DefaultJdbcUrl#create(JdbcUrlParser)}.
     */
    List<Map<String, String>> getHostInfo();

}
