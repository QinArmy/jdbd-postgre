package io.jdbd.postgre.config;

import io.jdbd.UrlException;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.env.JdbcUrlParser;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class PostgreUrlParser implements JdbcUrlParser {


    static PostgreUrlParser create(String url, Map<String, String> propMap) {
        return new PostgreUrlParser(url, propMap);
    }


    static final Pattern HOST_PATTERN = Pattern.compile(
            "(?:(?<host>[^/\\[?#;@:]+)|(?:\\[(?<hostIpv6>[^/?#;@]+)]))(?::(?<port>\\d+))?");

    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">postgre url</a>
     */
    static final Pattern URL_PATTERN = Pattern.compile(PgUrl.PROTOCOL
            + "(?://(?<hostList>[^,/?#;@]+(?:,[^,/?#;@]+)*)/)?" // 'host' or 'host:port'
            + "(?<database>[^/?#;]+)?/?"
            + "(?:\\?(?<query>[^#]*))?"
    );


    private final String originalUrl;

    private final Map<String, String> globalProperties;

    private final String dbName;

    private final List<Map<String, String>> hostInfoList;

    private PostgreUrlParser(final String url, Map<String, String> propMap) {
        this.originalUrl = Objects.requireNonNull(url, "url");
        final Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            String message = String.format("Postgre url[%s] syntax error,@see https://jdbc.postgresql.org/documentation/head/connect.html ."
                    , url);
            throw new UrlException(url, message);
        }
        this.globalProperties = createGlobalProperties(matcher, propMap);
        this.dbName = this.globalProperties.get(PgKey.PGDBNAME.getKey());
        this.hostInfoList = parseHostList(matcher.group("hostList"));
    }


    @Override
    public final String getOriginalUrl() {
        return this.originalUrl;
    }

    @Override
    public final String getProtocol() {
        return PgUrl.PROTOCOL;
    }

    @Override
    public final String getDbName() {
        return this.dbName;
    }

    @Override
    public final String getSubProtocol() {
        return null;
    }

    @Override
    public final Map<String, String> getGlobalProperties() {
        return this.globalProperties;
    }

    @Override
    public final List<Map<String, String>> getHostInfo() {
        return this.hostInfoList;
    }

    /**
     * @return a unmodifiable map
     */
    private Map<String, String> createGlobalProperties(final Matcher urlMatcher, final Map<String, String> propMap) {
        // 1. parse query
        final String query = urlMatcher.group("query");
        final String[] pairArray;
        if (query == null) {
            pairArray = new String[0];
        } else {
            pairArray = query.split("&");
        }

        final Map<String, String> globalMap = new HashMap<>((int) ((pairArray.length + propMap.size()) / 0.75F));
        // first parse query pair.
        PgStrings.parseQueryPair(this.originalUrl, pairArray, globalMap);

        // 2. put all propMap
        globalMap.putAll(propMap); // propMap can override query properties.

        // 3. put database
        String database;
        database = urlMatcher.group("database");
        if (database != null) {
            globalMap.put(PgKey.PGDBNAME.getKey(), PgStrings.decodeUrlPart(database));
        }
        return Collections.unmodifiableMap(globalMap);
    }

    /**
     * @return a unmodifiable list
     */
    private List<Map<String, String>> parseHostList(@Nullable String hostList) {
        final List<Map<String, String>> list;
        if (hostList == null) {
            list = Collections.singletonList(Collections.emptyMap());
        } else {
            String[] pairArray = hostList.split(",");
            List<Map<String, String>> tempList = new ArrayList<>(pairArray.length);
            Map<String, String> map;
            Matcher matcher;
            String host, port;
            for (String pair : pairArray) {
                matcher = HOST_PATTERN.matcher(pair);
                if (!matcher.matches()) {
                    String message = String.format("host[%s] error.", pair);
                    throw new UrlException(this.originalUrl, message);
                }
                host = matcher.group("host");
                if (host == null) {
                    host = matcher.group("hostIpv6");
                }
                map = new HashMap<>(4);
                map.put(PgKey.PGHOST.getKey(), PgStrings.decodeUrlPart(host));
                port = matcher.group("port");
                if (port != null) {
                    map.put(PgKey.PGPORT.getKey(), port);
                }
                tempList.add(Collections.unmodifiableMap(map));
            }

            list = PgCollections.unmodifiableList(tempList);
        }
        return list;
    }


}
