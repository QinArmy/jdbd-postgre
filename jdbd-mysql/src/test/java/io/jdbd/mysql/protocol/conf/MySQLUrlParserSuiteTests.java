package io.jdbd.mysql.protocol.conf;

import io.jdbd.UrlException;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.client.Enums;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Test(threadPoolSize = 3, groups = {Groups.MYSQL_URL})
public class MySQLUrlParserSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLUrlParserSuiteTests.class);

    @BeforeSuite
    public static void beforeSuite(ITestContext context) {
        LOG.info("\n\njdbd-mysql feature test suite start\n");

    }

    @AfterSuite
    public static void afterSuite(ITestContext context) {
        LOG.info("\njdbd-mysql feature test suite end.\n\n");
    }

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\nmysql url test start\n");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\nmysql url test end\n");
    }


    @Test
    public void singleConnection() {
        LOG.info("test SINGLE_CONNECTION start.");
        String url = "jdbc:mysql://192.168.0.106:3306/army?sslMode=REQUIRED";
        final Map<String, String> propMap = Collections.singletonMap(HostInfo.USER, "army_w");

        MySQLUrl mySQLUrl = MySQLUrl.getInstance(url, propMap);
        Assert.assertEquals(mySQLUrl.getProtocolType(), MySQLUrl.Protocol.SINGLE_CONNECTION, "protocolType");
        Assert.assertEquals(mySQLUrl.getProtocol(), MySQLUrl.Protocol.SINGLE_CONNECTION.getScheme(), "protocol");
        List<HostInfo<PropertyKey>> hostInfoList = mySQLUrl.getHostList();

        Assert.assertEquals(hostInfoList.size(), 1, "hostList size");
        HostInfo<PropertyKey> hostInfo = hostInfoList.get(0);
        Assert.assertEquals(hostInfo.getHost(), "192.168.0.106", "host");
        Assert.assertEquals(hostInfo.getPort(), MySQLUrl.DEFAULT_PORT, "port");

        Assert.assertEquals(hostInfo.getDbName(), "army");

        Properties<PropertyKey> properties = hostInfo.getProperties();
        Assert.assertEquals(properties.size(), 1, "prop size");
        Assert.assertEquals(properties.getOrDefault(PropertyKey.sslMode, Enums.SslMode.class), Enums.SslMode.REQUIRED, "sslMode");

        LOG.info("test SINGLE_CONNECTION end");
    }


    @Test
    public void failoverConnection() throws Exception {

        String protocol = MySQLUrl.Protocol.FAILOVER_CONNECTION.getScheme();
        String host1 = "address=(host=kafka)(port=3435)(key2=value2)";
        String host2 = "localhost:8080";
        String host3 = "( host  =  kosmo , port = 3306 )";
        String host4 = "( host  =  simonyi , port = 9987 )";
        String host5 = "zoro:3306";
        String host6 = "address=(host=myhost2)(port=2222)(key2=value2)";
        String dbName = "army";

        StringBuilder builder = new StringBuilder(protocol)
                .append("//")
                .append(host1)
                .append(",")
                .append(host2)

                .append(",")
                .append(host3)
                .append(",")
                .append(host4)
                .append(",")
                .append(host5)

                .append(",")
                .append(host6)
                .append("/")
                .append(dbName)
                .append("?sslMode=REQUIRED")
                .append("&")
                .append(URLEncoder.encode(PropertyKey.xdevapiSSLTrustStoreType.getKey(), StandardCharsets.UTF_8.name()))
                .append("=")
                .append(URLEncoder.encode(PropertyKey.xdevapiSSLTrustStoreType.getRequiredDefault(), StandardCharsets.UTF_8.name()));

        final String url = builder.toString();
        //LOG.info("url:{}", url);
        final Map<String, String> propMap = Collections.singletonMap(HostInfo.USER, "army");
        final long start = System.currentTimeMillis();
        MySQLUrl mySQLUrl = MySQLUrl.getInstance(url, propMap);
        LOG.info("mysql url parse cost {} ms", System.currentTimeMillis() - start);

        // global assert
        Assert.assertEquals(mySQLUrl.getOriginalUrl(), url, "url");
        Assert.assertEquals(mySQLUrl.getProtocolType(), MySQLUrl.Protocol.FAILOVER_CONNECTION, "protocolType");
        Assert.assertEquals(mySQLUrl.getProtocol(), MySQLUrl.Protocol.FAILOVER_CONNECTION.getScheme(), "protocol");
        Assert.assertNull(mySQLUrl.getSubProtocol(), "subProtocol");

        List<HostInfo<PropertyKey>> hostInfoList = mySQLUrl.getHostList();
        Assert.assertEquals(hostInfoList.size(), 6, "hostInfoList size");

        // host 1 assert
        HostInfo<PropertyKey> hostInfo1 = hostInfoList.get(0);

        Assert.assertEquals(hostInfo1.getUser(), propMap.get(HostInfo.USER), "host1 user");
        Assert.assertEquals(hostInfo1.getHost(), "kafka", "host1 host");
        Assert.assertEquals(hostInfo1.getPort(), 3435, "host1 port");
        Properties<PropertyKey> properties = hostInfo1.getProperties();

        Assert.assertEquals(properties.size(), 3, "host1 prop size");
        Assert.assertEquals(properties.getOrDefault(PropertyKey.sslMode, Enums.SslMode.class), Enums.SslMode.REQUIRED, " sslMode");
        Assert.assertEquals(properties.getOrDefault(PropertyKey.xdevapiSSLTrustStoreType), PropertyKey.xdevapiSSLTrustStoreType.getDefault(), "xdevapiSSLTrustStoreType");
        Assert.assertEquals(properties.getProperty("key2"), "value2", "host1 key2");

        Assert.assertEquals(hostInfo1.getDbName(), "army", "dbName");


        //host 2 assert
        HostInfo<PropertyKey> hostInfo2 = hostInfoList.get(1);

        Assert.assertEquals(hostInfo2.getUser(), propMap.get(HostInfo.USER), "host2 user");
        Assert.assertEquals(hostInfo2.getHost(), HostInfo.DEFAULT_HOST, "host2 host");
        Assert.assertEquals(hostInfo2.getPort(), 8080, "host2 port");
        properties = hostInfo2.getProperties();

        Assert.assertEquals(properties.size(), 2, "host2 prop size");

        //host 3 assert
        HostInfo<PropertyKey> hostInfo3 = hostInfoList.get(2);

        Assert.assertEquals(hostInfo3.getUser(), propMap.get(HostInfo.USER), "host3 user");
        Assert.assertEquals(hostInfo3.getHost(), "kosmo", "host3 host");
        Assert.assertEquals(hostInfo3.getPort(), MySQLUrl.DEFAULT_PORT, "host3 port");
        properties = hostInfo3.getProperties();

        Assert.assertEquals(properties.size(), 2, "host3 prop size");

        //host 4 assert
        HostInfo<PropertyKey> hostInfo4 = hostInfoList.get(3);

        Assert.assertEquals(hostInfo4.getUser(), propMap.get(HostInfo.USER), "host4 user");
        Assert.assertEquals(hostInfo4.getHost(), "simonyi", "host4 host");
        Assert.assertEquals(hostInfo4.getPort(), 9987, "host4 port");
        properties = hostInfo4.getProperties();

        Assert.assertEquals(properties.size(), 2, "host4 prop size");

        //host 5 assert
        HostInfo<PropertyKey> hostInfo5 = hostInfoList.get(4);

        Assert.assertEquals(hostInfo4.getUser(), propMap.get(HostInfo.USER), "host5 user");
        Assert.assertEquals(hostInfo5.getHost(), "zoro", "host5 host");
        Assert.assertEquals(hostInfo5.getPort(), MySQLUrl.DEFAULT_PORT, "host5 port");
        properties = hostInfo5.getProperties();

        Assert.assertEquals(properties.size(), 2, "host5 prop size");

        //host 6 assert
        HostInfo<PropertyKey> hostInfo6 = hostInfoList.get(5);

        Assert.assertEquals(hostInfo6.getUser(), propMap.get(HostInfo.USER), "host6 user");
        Assert.assertEquals(hostInfo6.getHost(), "myhost2", "host6 host");
        Assert.assertEquals(hostInfo6.getPort(), 2222, "host6 port");
        properties = hostInfo6.getProperties();

        Assert.assertEquals(properties.size(), 3, "host6 prop size");

        Assert.assertEquals(properties.getProperty("key2"), "value2", "host6 key2");


    }


    @Test
    public void userAndPasswordInUrl() {
        LOG.info("test userAndPasswordInUrl start.");
        final String url = "jdbc:mysql://army_w:army123@192.168.0.106:3306/army?sslMode=REQUIRED";

        MySQLUrl mySQLUrl = MySQLUrl.getInstance(url, Collections.emptyMap());

        Assert.assertEquals(mySQLUrl.getProtocolType(), MySQLUrl.Protocol.SINGLE_CONNECTION, "protocolType");
        Assert.assertEquals(mySQLUrl.getProtocol(), MySQLUrl.Protocol.SINGLE_CONNECTION.getScheme(), "schema");
        List<HostInfo<PropertyKey>> hostInfoList = mySQLUrl.getHostList();
        Assert.assertEquals(hostInfoList.size(), 1, "hostList size");

        HostInfo<PropertyKey> hostInfo = hostInfoList.get(0);
        Assert.assertEquals(hostInfo.getUser(), "army_w");
        Assert.assertEquals(hostInfo.getPassword(), "army123");
        Assert.assertEquals(hostInfo.getDbName(), "army");

        LOG.info("test userAndPasswordInUrl success.");
    }

    @Test
    public void defaultDatabase() {
        LOG.info("test defaultDatabase start.");
        final String url = "jdbc:mysql://army_w:army123@192.168.0.106:3306?sslMode=REQUIRED";

        MySQLUrl mySQLUrl = MySQLUrl.getInstance(url, Collections.emptyMap());

        Assert.assertTrue(MySQLStringUtils.isEmpty(mySQLUrl.getDbName()), "dbName");
        Assert.assertNull(mySQLUrl.getPrimaryHost().getDbName(), "host dbName");

        LOG.info("test defaultDatabase success.");
    }

    @Test
    public void defaultHostAndPort() {
        LOG.info("test defaultHostAndPort start.");
        String url = "jdbc:mysql://army_w:army123@?sslMode=REQUIRED";

        MySQLUrl mySQLUrl = MySQLUrl.getInstance(url, Collections.emptyMap());

        HostInfo<PropertyKey> hostInfo = mySQLUrl.getPrimaryHost();

        Assert.assertEquals(hostInfo.getHost(), HostInfo.DEFAULT_HOST, "host ");
        Assert.assertEquals(hostInfo.getPort(), MySQLUrl.DEFAULT_PORT, "pot");
        Assert.assertEquals(hostInfo.getUser(), "army_w");
        Assert.assertEquals(hostInfo.getPassword(), "army123");

        LOG.info("test defaultHostAndPort success.");
    }


    @Test(expectedExceptions = {UrlException.class})
    public void schemaError() {
        LOG.info("test schema error test start.");

        String url = "jdbc:oracle://192.168.0.106:3306/army?sslMode=REQUIRED";
        final Map<String, String> propMap = Collections.singletonMap(HostInfo.USER, "army_w");
        MySQLUrl.getInstance(url, propMap);

        LOG.info("test schema error test success.");
    }


}
