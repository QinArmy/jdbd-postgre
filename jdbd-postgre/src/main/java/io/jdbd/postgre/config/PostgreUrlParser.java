package io.jdbd.postgre.config;

import io.jdbd.config.UrlException;
import io.jdbd.postgre.util.PostgreStringUtils;
import io.jdbd.vendor.conf.JdbcUrlParser;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class PostgreUrlParser implements JdbcUrlParser {


    static PostgreUrlParser create(String url, Map<String, String> propMap) {
        return new PostgreUrlParser(url, propMap);
    }


    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">postgre url</a>
     */
    static final Pattern URL_PATTERN = Pattern.compile(PostgreUrl.PROTOCOL
            + "(?://(?:(?<host>(?:[^/?#;@:]+))|(?:\\[(?<hostIpv6>[^/?#;@]+)]))(?::(?<port>\\d+))?/)?" // 'host' or 'host:port'
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
        this.dbName = this.globalProperties.get(Property.PGDBNAME.getKey());
        this.hostInfoList = createHostInfoList();
    }


    @Override
    public final String getOriginalUrl() {
        return this.originalUrl;
    }

    @Override
    public final String getProtocol() {
        return PostgreUrl.PROTOCOL;
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
    private Map<String, String> createGlobalProperties(final Matcher matcher, final Map<String, String> propMap) {
        // 1. parse query
        final String query = matcher.group("query");
        final String[] pairArray;
        if (query == null) {
            pairArray = new String[0];
        } else {
            pairArray = query.split("&");
        }

        final Map<String, String> map = new HashMap<>((int) ((3 + pairArray.length + propMap.size()) / 0.75F));
        // first parse query pair.
        PostgreStringUtils.parseQueryPair(this.originalUrl, pairArray, map);

        // 2. put all propMap
        map.putAll(propMap); // propMap can override query properties.

        //3. parse host,port,database
        String hostStr;
        hostStr = matcher.group("host");
        if (hostStr == null) {
            hostStr = matcher.group("hostIpv6");
        }
        if (hostStr != null) {
            map.put(Property.PGHOST.getKey(), hostStr);
            map.put(Property.PGPORT.getKey(), matcher.group("port"));
        }

        String database;
        database = matcher.group("database");
        if (database != null) {
            map.put(Property.PGDBNAME.getKey(), database);
        }
        return Collections.unmodifiableMap(map);
    }

    private List<Map<String, String>> createHostInfoList() {
        return Collections.singletonList(Collections.emptyMap());
    }


}
