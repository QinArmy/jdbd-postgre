package io.jdbd.postgre.config;

import io.jdbd.postgre.Group;
import io.jdbd.postgre.env.PgUrlParser;
import io.jdbd.vendor.env.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.*;

/**
 * @see PgUrl
 */
@Test(groups = {Group.URL})
public class PgUrlSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgUrlSuiteTests.class);


    @Test
    public void urlParserDefaultHostAndPort() {
        LOG.info("{} group  urlParserDefaultHostAndPort test start", Group.URL);
        final String url = "jdbc:postgresql:army_test?user=army&password=army123&ssl=true";

        final PgUrl pgUrl = PgUrl.create(url, Collections.singletonMap("password", "qinarmy123"));

        assertNotNull(pgUrl, "postgreUrl");
        assertEquals(pgUrl.getProtocol(), PgUrlParser.PROTOCOL, "protocol");
        assertNull(pgUrl.getSubProtocol(), "sub protocol");
        assertEquals(pgUrl.getDbName(), "army_test", "database");

        final List<PgHost0> hostList = pgUrl.getHostList();
        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final PgHost0 host = hostList.get(0);
        assertEquals(host.getHost(), PgHost0.DEFAULT_HOST, "host");
        assertEquals(host.getPort(), PgHost0.DEFAULT_PORT, "port");
        assertEquals(host.getDbName(), "army_test");
        assertEquals(host.getUser(), "army", "user");

        assertNotEquals(host.getPassword(), "army123", "password");
        assertEquals(host.getPassword(), "qinarmy123", "password");
        assertFalse(host.isPasswordLess(), "isPasswordLess");

        final Properties properties = host.getProperties();
        assertNotNull(properties, "properties");

        assertNull(properties.get(PgKey0.user), "user");
        assertNull(properties.get(PgKey0.password), "password");
        assertNull(properties.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        LOG.info("{} group  urlParserDefaultHostAndPort test success", Group.URL);
    }

    @Test
    public void urlParserIpv6() throws Exception {
        LOG.info("{} group  urlParserIpv6 test start", Group.URL);
        final String userName = "秦军", database = "大秦";
        final String encodeUserName = URLEncoder.encode(userName, "UTF-8"), encodedDatabase = URLEncoder.encode(database, "UTF-8");
        final String url = String.format("jdbc:postgresql://[2001:DB8:0:23:8:800:200C:417A]:5432/%s/?user=%s&ssl=true", encodedDatabase, encodeUserName);

        final PgUrl pgUrl = PgUrl.create(url, Collections.singletonMap("password", "qinarmy123"));

        assertNotNull(pgUrl, "postgreUrl");
        assertEquals(pgUrl.getProtocol(), PgUrlParser.PROTOCOL, "protocol");
        assertNull(pgUrl.getSubProtocol(), "sub protocol");
        assertEquals(pgUrl.getDbName(), database, "database");

        final List<PgHost0> hostList = pgUrl.getHostList();
        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final PgHost0 host = hostList.get(0);
        assertEquals(host.getHost(), "2001:DB8:0:23:8:800:200C:417A", "host");
        assertEquals(host.getPort(), 5432, "port");
        assertEquals(host.getDbName(), database);
        assertEquals(host.getUser(), userName, "user");

        assertEquals(host.getPassword(), "qinarmy123", "password");
        assertFalse(host.isPasswordLess(), "isPasswordLess");

        final Properties properties = host.getProperties();
        assertNotNull(properties, "properties");

        assertNull(properties.get(PgKey0.user), "user");
        assertNull(properties.get(PgKey0.password), "password");
        assertNull(properties.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        LOG.info("{} group  urlParserIpv6 test success", Group.URL);
    }


    @Test
    public void urlParserIpv4() {
        LOG.info("{} group  urlParserIpv4 test start", Group.URL);
        final String url = "jdbc:postgresql://192.168.0.102:5432/army_test/?user=army&ssl=true";

        final PgUrl pgUrl = PgUrl.create(url, Collections.singletonMap("password", "qinarmy123"));

        assertNotNull(pgUrl, "postgreUrl");
        assertEquals(pgUrl.getProtocol(), PgUrlParser.PROTOCOL, "protocol");
        assertNull(pgUrl.getSubProtocol(), "sub protocol");
        assertEquals(pgUrl.getDbName(), "army_test", "database");

        final List<PgHost0> hostList = pgUrl.getHostList();
        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final PgHost0 host = hostList.get(0);
        assertEquals(host.getHost(), "192.168.0.102", "host");
        assertEquals(host.getPort(), 5432, "port");
        assertEquals(host.getDbName(), "army_test");
        assertEquals(host.getUser(), "army", "user");

        assertEquals(host.getPassword(), "qinarmy123", "password");
        assertFalse(host.isPasswordLess(), "isPasswordLess");

        final Properties properties = host.getProperties();
        assertNotNull(properties, "properties");

        assertNull(properties.get(PgKey0.user), "user");
        assertNull(properties.get(PgKey0.password), "password");
        assertNull(properties.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        LOG.info("{} group  urlParserIpv4 test success", Group.URL);
    }

    @Test
    public void failOverHostList() {
        LOG.info("{} group  failOverHostList test start", Group.URL);
        final String url = "jdbc:postgresql://192.168.0.102,[2001:DB8:0:23:8:800:200C:417A],localhost:7878,[2002:DB8:0:23:8:233:200C:417A]:5656/army_test/?user=army&ssl=true";

        final PgUrl pgUrl = PgUrl.create(url, Collections.singletonMap("password", "qinarmy123"));

        assertNotNull(pgUrl, "postgreUrl");
        assertEquals(pgUrl.getProtocol(), PgUrlParser.PROTOCOL, "protocol");
        assertNull(pgUrl.getSubProtocol(), "sub protocol");
        assertEquals(pgUrl.getDbName(), "army_test", "database");

        final List<PgHost0> hostList = pgUrl.getHostList();
        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 4, "hostList size");

        // host 0
        final PgHost0 host0 = hostList.get(0);
        assertEquals(host0.getHost(), "192.168.0.102", "host");
        assertEquals(host0.getPort(), PgHost0.DEFAULT_PORT, "port");
        assertEquals(host0.getDbName(), "army_test");
        assertEquals(host0.getUser(), "army", "user");

        assertEquals(host0.getPassword(), "qinarmy123", "password");
        assertFalse(host0.isPasswordLess(), "isPasswordLess");

        final Properties properties0 = host0.getProperties();
        assertNotNull(properties0, "properties");

        assertNull(properties0.get(PgKey0.user), "user");
        assertNull(properties0.get(PgKey0.password), "password");
        assertNull(properties0.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties0.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        // host 1
        final PgHost0 host1 = hostList.get(1);
        assertEquals(host1.getHost(), "2001:DB8:0:23:8:800:200C:417A", "host");
        assertEquals(host1.getPort(), PgHost0.DEFAULT_PORT, "port");
        assertEquals(host1.getDbName(), "army_test");
        assertEquals(host1.getUser(), "army", "user");

        assertEquals(host1.getPassword(), "qinarmy123", "password");
        assertFalse(host1.isPasswordLess(), "isPasswordLess");

        final Properties properties1 = host1.getProperties();
        assertNotNull(properties1, "properties");

        assertNull(properties1.get(PgKey0.user), "user");
        assertNull(properties1.get(PgKey0.password), "password");
        assertNull(properties1.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties1.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        // host 2
        final PgHost0 host2 = hostList.get(2);
        assertEquals(host2.getHost(), "localhost", "host");
        assertEquals(host2.getPort(), 7878, "port");
        assertEquals(host2.getDbName(), "army_test");
        assertEquals(host2.getUser(), "army", "user");

        assertEquals(host2.getPassword(), "qinarmy123", "password");
        assertFalse(host2.isPasswordLess(), "isPasswordLess");

        final Properties properties2 = host2.getProperties();
        assertNotNull(properties2, "properties");

        assertNull(properties2.get(PgKey0.user), "user");
        assertNull(properties2.get(PgKey0.password), "password");
        assertNull(properties2.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties2.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        // host 3
        final PgHost0 host3 = hostList.get(3);
        assertEquals(host3.getHost(), "2002:DB8:0:23:8:233:200C:417A", "host");
        assertEquals(host3.getPort(), 5656, "port");
        assertEquals(host3.getDbName(), "army_test");
        assertEquals(host3.getUser(), "army", "user");

        assertEquals(host3.getPassword(), "qinarmy123", "password");
        assertFalse(host3.isPasswordLess(), "isPasswordLess");

        final Properties properties3 = host3.getProperties();
        assertNotNull(properties3, "properties");

        assertNull(properties3.get(PgKey0.user), "user");
        assertNull(properties3.get(PgKey0.password), "password");
        assertNull(properties3.get(PgKey0.PGDBNAME), "dbName");
        assertEquals(properties3.getOrDefault(PgKey0.ssl, Boolean.class), Boolean.TRUE, "ssl");

        LOG.info("{} group  failOverHostList test success", Group.URL);
    }


}
