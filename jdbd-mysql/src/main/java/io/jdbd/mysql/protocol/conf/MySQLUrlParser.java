package io.jdbd.mysql.protocol.conf;

import io.jdbd.UrlException;
import io.jdbd.mysql.util.MySQLStringUtils;
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
 */
final class MySQLUrlParser {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLUrlParser.class);

    static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+)\\s*" // scheme: required; alphanumeric, plus, colon or percent
            + "(?://(?<authority>[^/?#]*))?\\s*" // authority: optional; starts with "//" followed by any char except "/", "?" and "#"
            + "(?:/(?!\\s*/)(?<path>[^?#]*))?" // path: optional; starts with "/" but not followed by "/", and then followed by by any char except "?" and "#"
            + "(?:\\?(?!\\s*\\?)(?<query>[^#]*))?" // query: optional; starts with "?" but not followed by "?", and then followed by by any char except "#"
            + "(?:\\s*#(?<fragment>.*))?"); // fragment: optional; starts with "#", and then followed by anything

    private static final Pattern HOST_PORT_HOST = Pattern.compile("(?<=^|,)(?:[^=,()]+(?::\\d+)?)(?=$|,)");
    private static final Pattern ADDRESS_EQUALS_HOST = Pattern.compile("(?:address=(?:\\([^=,()]+=[^=,()]+\\))+)");
    private static final Pattern KEY_VALUE_HOST = Pattern.compile("(?<![=)])(?:\\([^=,()]+=[^=,()]+(?:,[^=,()]+=[^=,()]+)*\\))");

    private static final Pattern SCHEME_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+).*");

    private final String originalUrl;
    private final String protocol;
    private final String authority;
    private final String path;
    private final String query;

    private final List<HostInfo> parsedHosts;
    private final Map<String, String> parsedProperties;

    /**
     * Static factory method for constructing instances of this class.
     *
     * @param connString The connection string to parse.
     * @return an instance of {@link MySQLUrlParser}
     */
    public static MySQLUrlParser parseConnectionString(String connString, Map<String, String> properties) {
        return new MySQLUrlParser(connString, properties);
    }

    /**
     * Constructs a connection string parser for the given connection string.
     *
     * @param connString the connection string to parse
     */
    private MySQLUrlParser(String connString, Map<String, String> properties) {
        if (connString == null) {
            throw new NullPointerException("connString");
        }
        if (!isConnectionStringSupported(connString)) {
            throw new UrlException("unsupported url schema", connString);
        }
        this.originalUrl = connString;
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(connString);
        if (!matcher.matches()) {
            throw new UrlException("url error.", connString);
        }
        this.protocol = decodeSkippingPlusSign(matcher.group("scheme"));
        this.authority = matcher.group("authority"); // Don't decode just yet.
        this.path = matcher.group("path") == null ? null : decode(matcher.group("path")).trim();
        this.query = matcher.group("query"); // Don't decode just yet.

        Map<String, String> parseProperties = parseQueryProperties();
        String actualAuthority = this.authority;
        if (!properties.containsKey(PropertyKey.USER.getKeyName())) {
            actualAuthority = parseUserInfo(parseProperties);
        }
        // override query properties.
        parseProperties.putAll(properties);
        this.parsedProperties = Collections.unmodifiableMap(parseProperties);

        // host info list
        if (MySQLStringUtils.hasText(actualAuthority)) {
            this.parsedHosts = parseHostList(actualAuthority);
        } else {
            this.parsedHosts = createDefaultHostList();
        }
    }

    public String getOriginalUrl() {
        return this.originalUrl;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public String getAuthority() {
        return this.authority;
    }

    public String getPath() {
        return this.path;
    }

    public String getQuery() {
        return this.query;
    }

    public List<HostInfo> getParsedHosts() {
        return this.parsedHosts;
    }

    public Map<String, String> getParsedProperties() {
        return this.parsedProperties;
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
        for (String pair : queryPairs) {
            String[] kv = pair.split("=");
            if (kv.length == 0 || kv.length > 2) {
                throw new UrlException(String.format("query[%s] error of url.", query), this.originalUrl);
            }
            if (kv.length == 2) {
                properties.put(kv[0].trim(), kv[1].trim());
            }
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
            throw new UrlException(String.format("user info[%s] error of url.", userInfo), this.originalUrl);
        }
        properties.put(PropertyKey.USER.getKeyName(), userInfoPair[0].trim());
        if (userInfoPair.length == 2) {
            properties.put(PropertyKey.PASSWORD.getKeyName(), userInfoPair[1].trim());
        }
        return authority.substring(index + 1);
    }


    /**
     * @return a unmodifiable list
     */
    private List<HostInfo> parseHostList(String multiHostsSegment) {
        if (multiHostsSegment.startsWith("[") && multiHostsSegment.endsWith("]")) {
            multiHostsSegment = multiHostsSegment.substring(1, multiHostsSegment.length() - 1);
        }
        final String authority = multiHostsSegment;
        final List<Character> markList = obtainMarkList();
        final int len = authority.length();

        List<HostInfo> hostList = new ArrayList<>();

        for (int openingMarkIndex = MySQLStringUtils.indexNonSpace(authority); openingMarkIndex > -1; ) {
            char openingMarker = authority.charAt(openingMarkIndex);
            if (openingMarker == '(') {
                // this 'if'  block for key-value host
                int index = authority.indexOf(')', openingMarkIndex);
                if (index < 0) {
                    throw new UrlException(authority.substring(openingMarkIndex) + " no closing mark"
                            , this.originalUrl);
                }
                index++; // right shift to comma or ending.
                hostList.add(parseKeyValueHost(authority.substring(openingMarkIndex, index)));
                index = MySQLStringUtils.indexNonSpace(authority, index);
                if (index < 0) {
                    break;
                }
                if (authority.charAt(index) != ',') {
                    throw createFormatException(authority.substring(openingMarkIndex, index + 1));
                }
                openingMarkIndex = MySQLStringUtils.indexNonSpace(authority, index + 1);
                if (openingMarkIndex < len) {
                    throw createAuthorityEndWithCommaException();
                }
            } else if (isAddressEqualsHostPrefix(authority, openingMarkIndex)) {
                // this 'if'  block for address equals host
                int commaIndex = indexAddressEqualsHostSegmentEnding(authority, openingMarkIndex);
                if (commaIndex < 0) {
                    hostList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex, commaIndex)));
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
                    hostList.add(parseHostPortHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostList.add(parseHostPortHost(authority.substring(openingMarkIndex, commaIndex)));
                    openingMarkIndex = MySQLStringUtils.indexNonSpace(authority, commaIndex + 1);
                    if (openingMarkIndex < 0) {
                        throw createAuthorityEndWithCommaException();
                    }
                }
            }
        }
        List<HostInfo> actualHostList;
        if (hostList.size() == 1) {
            actualHostList = Collections.singletonList(hostList.get(0));
        } else {
            actualHostList = new ArrayList<>(hostList.size());
            actualHostList.addAll(hostList);
            actualHostList = Collections.unmodifiableList(actualHostList);
        }
        return actualHostList;
    }


    private HostInfo parseAddressEqualsHost(String addressEqualsHost) {
        int openingMarkersIndex = addressEqualsHost.indexOf('(');
        if (openingMarkersIndex < 0) {
            throw new IllegalArgumentException(String.format("addressEqualsHost[%s] error.", addressEqualsHost));
        }

        final int originalOpeningMarkersIndex = openingMarkersIndex;
        int closingMarkersIndex;
        Map<String, String> hostKeyValueMap = new HashMap<>(this.parsedProperties);
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
        return createHostInfo(addressEqualsHost, hostKeyValueMap);
    }

    private HostInfo parseKeyValueHost(String keyValueHost) {
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
        Map<String, String> hostKeyValueMap = new HashMap<>(this.parsedProperties);
        for (String pair : pairArray) {
            String[] kv = pair.split("=");
            if (kv.length != 2) {
                throw createFormatException(keyValueHost);
            }
            hostKeyValueMap.put(kv[0].trim(), kv[1].trim());
        }
        return createHostInfo(keyValueHost, hostKeyValueMap);
    }

    private HostInfo parseHostPortHost(String hostPortHost) {
        String[] hostPortPair = hostPortHost.split(":");
        if (hostPortPair.length == 0 || hostPortPair.length > 2) {
            throw createFormatException(hostPortHost);
        }
        Map<String, String> hostKeyValueMap = new HashMap<>(this.parsedProperties);
        hostKeyValueMap.put("host", hostPortPair[0].trim());

        if (hostPortPair.length == 2) {
            hostKeyValueMap.put("port", hostPortPair[1].trim());
        }
        return createHostInfo(hostPortHost, hostKeyValueMap);
    }


    private HostInfo createHostInfo(String hostInfo, Map<String, String> hostKeyValueMap) {

        String host = hostKeyValueMap.remove("host");
        if (MySQLStringUtils.isEmpty(host)) {
            throw new UrlException(String.format("hostInfo[%s] not found host.", hostInfo), this.originalUrl);
        }
        String portText = hostKeyValueMap.remove("port");
        int port = MySQLUrl.DEFAULT_PORT;
        if (portText != null) {
            port = parsePort(portText);
            if (port < 0) {
                throw new UrlException(String.format("hostInfo[%s] port error.", hostInfo), this.originalUrl);
            }
        }

        String user = hostKeyValueMap.remove("user");
        if (!MySQLStringUtils.hasText(user)) {
            throw new UrlException("not found user info.", this.originalUrl);
        }
        String password = hostKeyValueMap.remove("password");
        return new HostInfo(this.originalUrl, host, port, user, password, hostKeyValueMap);
    }

    /**
     * @return port or negative integer.
     */
    private int parsePort(String portText) {
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            port = -1;
        }
        return port;
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


    private List<HostInfo> createDefaultHostList() {
        Map<String, String> props = new HashMap<>(this.parsedProperties);
        props.put("host", MySQLUrl.DEFAULT_HOST);
        props.put("port", Integer.toString(MySQLUrl.DEFAULT_PORT));
        return Collections.singletonList(createHostInfo("", props));
    }

    private UrlException createAuthorityEndWithCommaException() {
        throw new UrlException(String.format("\"%s\" can't end with comma", this.authority), this.originalUrl);
    }

    private UrlException createParenthesisNotMatchException(String hostSegment) {
        return new UrlException(String.format("\"%s\" parenthesis count not match.", hostSegment), this.originalUrl);
    }

    private UrlException createFormatException(String hostSegment) {
        return new UrlException(String.format("\"%s\" format error.", hostSegment), this.originalUrl);
    }


    /**
     * @deprecated use {@link #parseHostList(String)} ,because check comma.
     */
    @Deprecated
    private List<HostInfo> parseHostListByPattern(final String actualAuthority) {
        String tempAuthority = actualAuthority.replaceAll("\\s", "");
        if (tempAuthority.startsWith("[") && tempAuthority.endsWith("]")) {
            tempAuthority = tempAuthority.substring(1, tempAuthority.length() - 1);
        }
        final String authority = tempAuthority;
        if (authority.isEmpty()) {
            String user = this.parsedProperties.get(PropertyKey.USER.getKeyName());
            String password = this.parsedProperties.get(PropertyKey.USER.getKeyName());
            return Collections.singletonList(new HostInfo(this.originalUrl, user, password));
        }
        Matcher matcher;
        Map<Integer, HostInfo> hostInfoMap = new HashMap<>();
        final int len = authority.length();
        int index = 0;
        matcher = ADDRESS_EQUALS_HOST.matcher(authority);
        while (index < len) {
            if (matcher.find(index)) {
                index = matcher.end();
                hostInfoMap.put(matcher.start(), parseAddressEqualsHost(matcher.group()));
            } else {
                break;
            }
        }
        index = 0;
        matcher = KEY_VALUE_HOST.matcher(authority);
        while (index < len) {
            if (matcher.find(index)) {
                hostInfoMap.put(matcher.start(), parseKeyValueHost(matcher.group()));
                index = matcher.end();
            } else {
                break;
            }
        }
        index = 0;
        matcher = HOST_PORT_HOST.matcher(authority);
        while (index < len) {
            if (matcher.find(index)) {
                hostInfoMap.put(matcher.start(), parseHostPortHost(matcher.group()));
                index = matcher.end();
            } else {
                break;
            }
        }
        List<Integer> startIndexList = new ArrayList<>(hostInfoMap.keySet());
        startIndexList.sort(Comparator.comparingInt(Integer::intValue));

        List<HostInfo> hostInfoList = new ArrayList<>(startIndexList.size());
        for (Integer startIndex : startIndexList) {
            hostInfoList.add(hostInfoMap.get(startIndex));
        }
        return hostInfoList;
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
        if (connString == null) {
            throw new NullPointerException("connString");
        }
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
        if (text == null || text.equals("")) {
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
