package io.jdbd.mysql.protocol.conf;


import io.jdbd.JdbdException;
import io.jdbd.mysql.env.Protocol;
import io.jdbd.mysql.protocol.client.Charsets;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.vendor.env.*;
import io.qinarmy.env.convert.ConverterManager;
import io.qinarmy.env.convert.ImmutableConverterManager;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrl}
 * </p>
 */
public final class MySQLUrl extends AbstractJdbcUrl {

    public static final int DEFAULT_PORT = 3306;

    public static MySQLUrl getInstance(final String url, final Map<String, Object> properties) {
        final MySQLUrl mySQLUrl;
        mySQLUrl = new MySQLUrl(MySQLUrlParser.parseMySQLUrl(url, properties));
        checkUrlProperties(mySQLUrl.getCommonProps());
        for (MySQLHost0 host : mySQLUrl.hostList) {
            Properties props = host.getProperties();
            checkUrlProperties(props);
            checkKeyProperties(props);
        }


        return mySQLUrl;
    }

    public static boolean acceptsUrl(final String url) {
        boolean accept = false;
        for (Protocol protocol : Protocol.values()) {
            if (url.startsWith(protocol.scheme)) {
                accept = true;
                break;
            }
        }
        return accept;
    }

    public final Protocol protocolType;

    private final List<MySQLHost0> hostList;

    private final int maxAllowedPayload;

    private MySQLUrl(MySQLUrlParser parser) {
        super(parser);
        this.hostList = createHostInfoList(parser);
        this.protocolType = Protocol.fromValue(this.getOriginalUrl(), this.getProtocol(), this.getHostList().size());
        this.maxAllowedPayload = parseMaxAllowedPacket();
    }

    @Override
    public MySQLHost0 getPrimaryHost() {
        return this.hostList.get(0);
    }

    @Override
    public List<MySQLHost0> getHostList() {
        return this.hostList;
    }

    public int getMaxAllowedPayload() {
        return this.maxAllowedPayload;
    }

    @Override
    protected String internalToString() {
        return this.protocolType.name();
    }


    @Override
    protected PropertyKey getDbNameKey() {
        return MyKey.dbname;
    }

    @Override
    protected Properties createProperties(Map<String, String> map) {
        return wrapProperties(map);
    }


    /**
     * @see MyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private int parseMaxAllowedPacket() {
        throw new UnsupportedOperationException();
    }

    public static Properties wrapProperties(Map<String, String> map) {
        ConverterManager converterManager = ImmutableConverterManager.create(Converters::registerConverter);
        return ImmutableMapProperties.getInstance(map, converterManager);
    }

    private static List<MySQLHost0> createHostInfoList(JdbcUrlParser parser) {
        final List<Map<String, Object>> hostMapList = parser.getHostInfo();

        final int hostSize = hostMapList.size();
        List<MySQLHost0> hostInfoList = MySQLCollections.arrayList(hostSize);
        for (int i = 0; i < hostSize; i++) {
            hostInfoList.add(MySQLHost0.create(parser, i));
        }
        return hostInfoList;
    }

    private static void checkUrlProperties(final Properties properties) {
        for (MyKey key : MyKey.values()) {
            properties.get(key, key.getJavaType());
        }
    }

    private static void checkKeyProperties(final Properties properties) {
        if (properties.getOrDefault(MyKey.factoryWorkerCount, Integer.class) < 1) {
            throw errorPropertyValue(MyKey.factoryWorkerCount);
        }
        final Charset charset;
        charset = properties.get(MyKey.characterEncoding, Charset.class);
        if (charset != null && Charsets.isUnsupportedCharsetClient(charset.name())) {
            String m = String.format(
                    "Property[%s] value[%s] isn unsupported client charset,because encode US_ASCII as multi bytes.",
                    MyKey.characterEncoding, charset.name());
            throw new JdbdException(m);
        }


    }

    private static JdbdException errorPropertyValue(final MyKey key) {
        String m = String.format("Property[%s] value error.", key);
        return new JdbdException(m);
    }


}
