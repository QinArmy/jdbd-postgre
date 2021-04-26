package io.jdbd.mysql.protocol.conf;

import io.jdbd.config.UrlException;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.JdbcUrlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrlParser}
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html">Connection URL Syntax</a>
 */
final class MySQLUrlParser implements JdbcUrlParser {

    /**
     * Static factory method for constructing instances of this class.
     *
     * @param connString The connection string to parse.
     * @return an instance of {@link MySQLUrlParser}
     */
    static MySQLUrlParser parseMySQLUrl(String connString, Map<String, String> properties) {
        return new MySQLUrlParser(connString, properties);
    }


    static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+)\\s*" // scheme: required; alphanumeric, plus, colon or percent
            + "(?://(?<authority>[^/?#]*))?\\s*" // authority: optional; starts with "//" followed by any char except "/", "?" and "#"
            + "(?:/(?!\\s*/)(?<path>[^?#]*))?" // path: optional; starts with "/" but not followed by "/", and then followed by by any char except "?" and "#"
            + "(?:\\?(?!\\s*\\?)(?<query>[^#]*))?" // query: optional; starts with "?" but not followed by "?", and then followed by by any char except "#"
            + "(?:\\s*#(?<fragment>.*))?"); // fragment: optional; starts with "#", and then followed by anything


    private static final Pattern SCHEME_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+).*");

    private final Logger LOG = LoggerFactory.getLogger(MySQLUrlParser.class);

    private final String originalUrl;
    private final String schema;
    private final String authority;
    private final String path;
    private final String query;

    private final List<Map<String, String>> hostInfo;

    private final Map<String, String> globalProperties;

    /**
     * Constructs a connection string parser for the given connection string.
     *
     * @param connString the connection string to parse
     */
    private MySQLUrlParser(String connString, final Map<String, String> properties) {
        if (!isConnectionStringSupported(connString)) {
            throw new UrlException(connString, "unsupported url schema");
        }
        this.originalUrl = connString;
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(connString);
        if (!matcher.matches()) {
            throw new UrlException(connString, "url schema not match.");
        }
        // 1. parse url partition.
        this.schema = decodeSkippingPlusSign(matcher.group("scheme"));
        this.authority = matcher.group("authority"); // Don't decode just yet.
        this.path = matcher.group("path") == null ? null : decode(matcher.group("path")).trim();
        this.query = matcher.group("query"); // Don't decode just yet.

        // 2-1 parse url query properties
        Map<String, String> parseProperties = parseQueryProperties();

        //2-2 parse user and password from url
        String actualAuthority = this.authority;
        if (!properties.containsKey(PropertyKey.user.getKey())) {
            actualAuthority = parseUserInfo(parseProperties);
        }
        // override query properties wih host
        parseProperties.putAll(properties);

        //3. create global properties
        int capacity = (int) ((properties.size() + parseProperties.size()) * 0.75F);
        final Map<String, String> globalProperties = new HashMap<>(capacity);
        //firstly query properties
        globalProperties.putAll(parseProperties);
        //secondly properties
        globalProperties.putAll(properties);
        // thirdly dbname
        if (this.path == null) {
            globalProperties.remove(PropertyKey.dbname.getKey());
        } else {
            globalProperties.put(PropertyKey.dbname.getKey(), this.path);
        }
        this.globalProperties = Collections.unmodifiableMap(globalProperties);

        //4. parse host info list
        if (MySQLStringUtils.hasText(actualAuthority)) {
            this.hostInfo = parseHostList(actualAuthority);
        } else {
            this.hostInfo = createDefaultHostList();
        }
    }

    @Override
    public String getSubProtocol() {
        return null;
    }

    @Override
    public Map<String, String> getGlobalProperties() {
        return this.globalProperties;
    }

    @Override
    public List<Map<String, String>> getHostInfo() {
        return this.hostInfo;
    }

    @Override
    public String getOriginalUrl() {
        return this.originalUrl;
    }

    @Override
    public String getProtocol() {
        return this.schema;
    }

    public String getAuthority() {
        return this.authority;
    }

    @Override
    public String getDbName() {
        return this.path;
    }

    public String getQuery() {
        return this.query;
    }


    /**
     * @return a modifiable map
     */
    private Map<String, String> parseQueryProperties() {
        String query = this.query;
        if (MySQLStringUtils.isEmpty(query)) {
            return new HashMap<>();
        }
        String[] queryPairs = query.split("&");

        Map<String, String> properties = new HashMap<>();
        try {
            for (String pair : queryPairs) {
                String[] kv = pair.split("=");
                if (kv.length == 0 || kv.length > 2) {
                    throw new UrlException(String.format("query[%s] error of url.", query), this.originalUrl);
                }
                if (kv.length == 2) {
                    properties.put(URLDecoder.decode(kv[0], "UTF-8"), URLDecoder.decode(kv[1], "UTF-8"));
                }
            }
        } catch (UnsupportedEncodingException e) {
            // use UTF-8 never here
            throw new UrlException(this.originalUrl, "Unsupported charset", e);
        }
        return properties;
    }

    private String parseUserInfo(Map<String, String> properties) {
        String authority = this.authority;
        int index = authority.indexOf('@');
        if (index < 0) {
            return authority;
        }
        String userInfo = authority.substring(0, index);
        String[] userInfoPair = userInfo.split(":");
        if (userInfoPair.length == 0 || userInfoPair.length > 2) {
            String message = String.format("user info[%s] error of url.", userInfo);
            throw new UrlException(this.originalUrl, message);
        }
        try {
            properties.put(PropertyKey.user.getKey(), URLDecoder.decode(userInfoPair[0], "UTF-8"));
            if (userInfoPair.length == 2) {
                properties.put(PropertyKey.password.getKey(), URLDecoder.decode(userInfoPair[1], "UTF-8"));
            }
        } catch (UnsupportedEncodingException e) {
            //never here
            throw new UrlException(this.originalUrl, "Unsupported charset.", e);
        }
        return authority.substring(index + 1);
    }


    /**
     * @return a unmodifiable list
     */
    private List<Map<String, String>> parseHostList(String multiHostsSegment) {
        if (multiHostsSegment.startsWith("[") && multiHostsSegment.endsWith("]")) {
            multiHostsSegment = multiHostsSegment.substring(1, multiHostsSegment.length() - 1);
        }
        final String authority = multiHostsSegment;
        final List<Character> markList = obtainMarkList();
        final int len = authority.length();

        List<Map<String, String>> hostPropertiesList = new ArrayList<>();

        for (int openingMarkIndex = MySQLStringUtils.indexNonSpace(authority); openingMarkIndex > -1; ) {
            char openingMarker = authority.charAt(openingMarkIndex);
            if (openingMarker == '(') {
                // this 'if'  block for key-value host
                int index = authority.indexOf(')', openingMarkIndex);
                if (index < 0) {
                    String message = String.format("%s no closing mark", authority.substring(openingMarkIndex));
                    throw new UrlException(this.originalUrl, message);
                }
                index++; // right shift to comma or ending.
                hostPropertiesList.add(parseKeyValueHost(authority.substring(openingMarkIndex, index)));
                index = MySQLStringUtils.indexNonSpace(authority, index);
                if (index < 0) {
                    break;
                }
                if (authority.charAt(index) != ',') {
                    throw createFormatException(authority.substring(openingMarkIndex, index + 1));
                }
                openingMarkIndex = MySQLStringUtils.indexNonSpace(authority, index + 1);
                if (openingMarkIndex < 0) {
                    throw createAuthorityEndWithCommaException();
                }
            } else if (isAddressEqualsHostPrefix(authority, openingMarkIndex)) {
                // this 'if'  block for address equals host
                int commaIndex = indexAddressEqualsHostSegmentEnding(authority, openingMarkIndex);
                if (commaIndex < 0) {
                    hostPropertiesList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostPropertiesList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex, commaIndex)));
                    openingMarkIndex = MySQLStringUtils.indexNonSpace(authority, commaIndex + 1);
                    if (openingMarkIndex < 0) {
                        throw createAuthorityEndWithCommaException();
                    }
                }
            } else if (markList.contains(authority.charAt(openingMarkIndex))) {
                throw createFormatException(authority.substring(openingMarkIndex == 0 ? 0 : openingMarkIndex - 1));
            } else {
                // this 'else'  block for host-port host
                int commaIndex = -1;
                for (int i = openingMarkIndex + 1; i < len; i++) {
                    char ch = authority.charAt(i);
                    if (ch == ',') {
                        commaIndex = i;
                        break;
                    } else if (markList.contains(ch)) {
                        throw createFormatException(authority.substring(openingMarkIndex, i + 1));
                    }
                }
                if (commaIndex < 0) {
                    hostPropertiesList.add(parseHostPortHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostPropertiesList.add(parseHostPortHost(authority.substring(openingMarkIndex, commaIndex)));
                    openingMarkIndex = MySQLStringUtils.indexNonSpace(authority, commaIndex + 1);
                    if (openingMarkIndex < 0) {
                        throw createAuthorityEndWithCommaException();
                    }
                }
            }
        }
        List<Map<String, String>> actualHostList;
        if (hostPropertiesList.size() == 1) {
            actualHostList = Collections.singletonList(hostPropertiesList.get(0));
        } else {
            actualHostList = new ArrayList<>(hostPropertiesList.size());
            actualHostList.addAll(hostPropertiesList);
            actualHostList = Collections.unmodifiableList(actualHostList);
        }
        return actualHostList;
    }


    /**
     * @return a unmodifiable map
     */
    private Map<String, String> parseAddressEqualsHost(String addressEqualsHost) {
        int openingMarkersIndex = addressEqualsHost.indexOf('(');
        if (openingMarkersIndex < 0) {
            throw new IllegalArgumentException(String.format("addressEqualsHost[%s] error.", addressEqualsHost));
        }

        final int originalOpeningMarkersIndex = openingMarkersIndex;
        int closingMarkersIndex;
        Map<String, String> hostKeyValueMap = new HashMap<>();
        while ((closingMarkersIndex = addressEqualsHost.indexOf(')', openingMarkersIndex)) > 0) {
            String pair = addressEqualsHost.substring(openingMarkersIndex + 1, closingMarkersIndex);
            String[] kv = pair.split("=");
            if (kv.length != 2) {
                throw createFormatException(addressEqualsHost);
            }
            hostKeyValueMap.put(kv[0].trim(), kv[1].trim());
            openingMarkersIndex = addressEqualsHost.indexOf('(', closingMarkersIndex);
            if (openingMarkersIndex < 0) {
                break;
            }
        }
        if (openingMarkersIndex == originalOpeningMarkersIndex) {
            // not found key value pair.
            throw createFormatException(addressEqualsHost);
        }
        return MySQLCollections.unmodifiableMap(hostKeyValueMap);
    }

    /**
     * @return a unmodifiable map
     */
    private Map<String, String> parseKeyValueHost(String keyValueHost) {
        int openingMarkersIndex = keyValueHost.indexOf('(');
        int closingMarkersIndex = keyValueHost.lastIndexOf(')');

        if (openingMarkersIndex < 0 || closingMarkersIndex < 0) {
            throw createFormatException(keyValueHost);
        }
        keyValueHost = keyValueHost.substring(openingMarkersIndex + 1, closingMarkersIndex);
        String[] pairArray = keyValueHost.split(",");

        if (pairArray.length == 0) {
            throw createFormatException(keyValueHost);
        }
        Map<String, String> hostKeyValueMap = new HashMap<>((int) (pairArray.length / 0.75F));
        for (String pair : pairArray) {
            String[] kv = pair.split("=");
            if (kv.length != 2) {
                throw createFormatException(keyValueHost);
            }
            hostKeyValueMap.put(kv[0].trim(), kv[1].trim());
        }
        return MySQLCollections.unmodifiableMap(hostKeyValueMap);
    }

    /**
     * @return a unmodifiable map
     */
    private Map<String, String> parseHostPortHost(String hostPortHost) {
        String[] hostPortPair = hostPortHost.split(":");
        if (hostPortPair.length == 0 || hostPortPair.length > 2) {
            throw createFormatException(hostPortHost);
        }
        Map<String, String> hostKeyValueMap = new HashMap<>(4);
        hostKeyValueMap.put(PropertyKey.host.getKey(), hostPortPair[0].trim());

        if (hostPortPair.length == 2) {
            hostKeyValueMap.put(PropertyKey.port.getKey(), hostPortPair[1].trim());
        }
        return Collections.unmodifiableMap(hostKeyValueMap);
    }


    private List<Character> obtainMarkList() {
        List<Character> chList = new ArrayList<>(4);
        chList.add(',');
        chList.add('=');
        chList.add('(');
        chList.add(')');
        return Collections.unmodifiableList(chList);
    }

    /**
     * @param openingMark index of address-equals host prefix{@code pattern 'address\s*=\s*('}
     */
    private int indexAddressEqualsHostSegmentEnding(final String authority, final int openingMark) {
        final int prefixEndIndex = authority.indexOf('(', openingMark);
        if (prefixEndIndex < 0) {
            throw new IllegalArgumentException("openingMark isn't address-equals host prefix index.");
        }
        final int len = authority.length();
        boolean close = false;
        int separator = -1;
        for (int i = prefixEndIndex + 1; i < len; i++) {
            if (authority.charAt(i) == ')') {
                if (close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                close = true;
            } else if (authority.charAt(i) == '(') {
                if (!close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                close = false;
            } else if (authority.charAt(i) == ',') {
                if (!close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                separator = i;
                break;
            }
        }
        if (!close) {
            throw createParenthesisNotMatchException(authority.substring(openingMark));
        }
        return separator;
    }


    private List<Map<String, String>> createDefaultHostList() {
        Map<String, String> props = new HashMap<>(4);
        props.put(PropertyKey.host.getKey(), HostInfo.DEFAULT_HOST);
        props.put(PropertyKey.port.getKey(), Integer.toString(MySQLUrl.DEFAULT_PORT));
        return Collections.singletonList(props);
    }

    private UrlException createAuthorityEndWithCommaException() {
        String message = String.format("\"%s\" can't end with comma", this.authority);
        throw new UrlException(this.originalUrl, message);
    }

    private UrlException createParenthesisNotMatchException(String hostSegment) {
        String message = String.format("\"%s\" parenthesis count not match.", hostSegment);
        return new UrlException(this.originalUrl, message);
    }

    private UrlException createFormatException(String hostSegment) {
        String message = String.format("\"%s\" format error.", hostSegment);
        return new UrlException(this.originalUrl, message);
    }


    /*################################## blow static method ##################################*/

    static boolean isAddressEqualsHostPrefix(String segment, final int fromIndex) {
        final String address = "address";
        if (fromIndex < 0 || !segment.startsWith(address, fromIndex)) {
            return false;
        }
        final int len = segment.length();
        final char space = '\u0020';
        char ch;
        boolean match = false;
        for (int i = fromIndex + address.length(), charCount = 0; i < len; i++) {
            ch = segment.charAt(i);
            if (ch == space) {
                continue;
            }
            if (ch == '=' && charCount == 0) {
                charCount++;
            } else if (ch == '(' && charCount == 1) {
                match = true;
                break;
            } else {
                break;
            }
        }
        return match;
    }


    /**
     * Checks if the scheme part of given connection string matches one of the {@link io.jdbd.mysql.protocol.conf.MySQLUrl.Protocol}s supported by Connector/J.
     *
     * @param connString connection string
     * @return true if supported
     */
    public static boolean isConnectionStringSupported(String connString) {
        Matcher matcher = SCHEME_PTRN.matcher(connString);
        return matcher.matches() && MySQLUrl.Protocol.isSupported(decodeSkippingPlusSign(matcher.group("scheme")));
    }


    /**
     * URL-decode the given string skipping all occurrences of the plus sign.
     *
     * @param text the string to decode
     * @return the decoded string
     */
    private static String decodeSkippingPlusSign(String text) {
        if (text.equals("")) {
            return text;
        }
        text = text.replace("+", "%2B"); // Percent encode for "+" is "%2B".
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
        }
        return "";
    }

    /**
     * URL-decode the given string.
     *
     * @param text the string to decode
     * @return the decoded string
     */
    private static String decode(String text) {
        if (MySQLStringUtils.isEmpty(text)) {
            return text;
        }
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
        }
        return "";
    }


}
