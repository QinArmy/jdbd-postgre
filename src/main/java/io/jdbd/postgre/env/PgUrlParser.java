package io.jdbd.postgre.env;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * This util class is responsible for parsing url. see {@link io.jdbd.Driver#forPoolVendor(String, Map)}
 * </p>
 *
 * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">Postgre url</a>
 * @since 1.0
 */
public abstract class PgUrlParser {

    private PgUrlParser() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">Postgre url</a>
     */
    public static final String PROTOCOL = "jdbd:postgresql:";

    private static final Pattern HOST_PATTERN = Pattern.compile(
            "(?:(?<host>[^/\\[?#;@:]+)|(?:\\[(?<hostIpv6>[^/?#;@]+)]))(?::(?<port>\\d+))?");

    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">postgre url</a>
     */
    private static final Pattern URL_PATTERN = Pattern.compile(PgUrlParser.PROTOCOL
            + "(?://(?<hostList>[^,/?#;@]+(?:,[^,/?#;@]+)*)/)?" // 'host' or 'host:port'
            + "(?<database>[^/?#;]+)?/?"
            + "(?:\\?(?<query>[^#]*))?"
    );


    /**
     * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">Postgre url</a>
     */
    public static List<PgHost> parse(final @Nullable String url, final @Nullable Map<String, Object> properties) {
        if (url == null) {
            throw new NullPointerException("url must non-null");
        } else if (properties == null) {
            throw new NullPointerException("properties must non-null");
        }
        final Matcher matcher;
        matcher = URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            String m = String.format("Postgre url[%s] syntax error,\n@see https://jdbc.postgresql.org/documentation/head/connect.html .", url);
            throw new JdbdException(m);
        }
        final Map<String, Object> globalProperties;
        globalProperties = parseUrlProperties(url, matcher, properties);

        return parseHostList(url, matcher.group("hostList"), globalProperties);
    }

    /**
     * @return a unmodifiable map
     */
    private static Map<String, Object> parseUrlProperties(final String url, final Matcher urlMatcher, final Map<String, Object> propMap) {
        // 1. parse query
        final String query = urlMatcher.group("query");
        final String[] pairArray;
        if (query == null) {
            pairArray = new String[0];
        } else {
            pairArray = query.split("&");
        }

        final Map<String, Object> globalMap;
        globalMap = PgCollections.hashMap((int) ((pairArray.length + propMap.size()) / 0.75F));
        // first parse query pair.
        PgStrings.parseQueryPair(url, pairArray, globalMap);

        // 2. put all propMap
        globalMap.putAll(propMap); // propMap can override query properties.

        // 3. put database
        String database;
        database = urlMatcher.group("database");
        if (database != null) {
            globalMap.put(PgHost.DB_NAME, PgStrings.decodeUrlPart(database));
        }
        return Collections.unmodifiableMap(globalMap);
    }

    /**
     * @return a unmodifiable list
     * @see <a href="https://jdbc.postgresql.org/documentation/use/#connection-fail-over">Connection Fail-over </a>
     */
    private static List<PgHost> parseHostList(final String url, final @Nullable String hostList,
                                              final Map<String, Object> globalProperties) {
        Map<String, Object> map;
        if (!PgStrings.hasText(hostList)) {
            map = PgCollections.hashMap(globalProperties);
            map.put(PgHost.HOST, PgHost.DEFAULT_HOST);
            map.put(PgHost.PORT, PgHost.DEFAULT_PORT);
            return Collections.singletonList(PostgreHost.create(map));
        }
        final String[] pairArray = hostList.split(",");
        final List<PgHost> pgHostList = PgCollections.arrayList(pairArray.length);

        Matcher matcher;
        String host, port;
        for (String pair : pairArray) {
            matcher = HOST_PATTERN.matcher(pair);
            if (!matcher.matches()) {
                String message = String.format("host[%s] error in url %s", pair, url);
                throw new JdbdException(message);
            }
            host = matcher.group("host");
            if (host == null) {
                host = matcher.group("hostIpv6");
            }

            map = PgCollections.hashMap(globalProperties);

            if (PgStrings.hasText(host)) {
                map.put(PgHost.HOST, PgStrings.decodeUrlPart(host));
            } else {
                map.put(PgHost.HOST, PgHost.DEFAULT_HOST);
            }

            port = matcher.group(PgHost.PORT);

            if (PgStrings.hasText(port)) {
                map.put(PgHost.PORT, PgStrings.parsePort(url, port));
            } else {
                map.put(PgHost.PORT, PgHost.DEFAULT_PORT);
            }
            pgHostList.add(PostgreHost.create(map));
        }

        return PgCollections.unmodifiableList(pgHostList);
    }


}
