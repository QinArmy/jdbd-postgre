package io.jdbd.postgre.config;


import io.jdbd.config.UrlException;
import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * @see PostgreUrlParser
 */
@Test(groups = {Group.URL})
public class PostgreUrlParserSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreUrlParserSuiteTests.class);


    @Test
    public void urlParserDefaultHostAndPort() {
        LOG.info("{} group  urlParserDefaultHostAndPort test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql:army_test?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertEquals(parser.getDbName(), "army_test", postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");

        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);
        assertTrue(host.isEmpty(), "host");

        LOG.info("{} group  urlParserDefaultHostAndPort test success", Group.URL);
    }


    @Test
    public void urlParserIpv6() throws Exception {
        LOG.info("{} group  urlParserIpv6 test start", Group.URL);

        final String userName = "秦军", database = "大秦";
        final String encodeUserName = URLEncoder.encode(userName, "UTF-8"), encodedDatabase = URLEncoder.encode(database, "UTF-8");
        final String url = String.format("jdbc:postgresql://[2001:DB8:0:23:8:800:200C:417A]:5432/%s/?user=%s", encodedDatabase, encodeUserName);
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(url, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), url, url);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, url);
        assertNull(parser.getSubProtocol(), url);
        assertEquals(parser.getDbName(), database, url);


        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("user"), userName, "user");

        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);

        assertEquals(host.get(PgKey.PGHOST.getKey()), "2001:DB8:0:23:8:800:200C:417A", "host");
        assertEquals(host.get(PgKey.PGPORT.getKey()), "5432", "port");

        LOG.info("{} group  urlParserIpv6 test success", Group.URL);
    }

    @Test
    public void urlParserIpv4() {
        LOG.info("{} group  urlParserIpv4 test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql://192.168.0.102:5432/army_test/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertEquals(parser.getDbName(), "army_test", postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");

        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);

        assertEquals(host.get(PgKey.PGHOST.getKey()), "192.168.0.102", "host");
        assertEquals(host.get(PgKey.PGPORT.getKey()), "5432", "port");


        LOG.info("{} group  urlParserIpv4 test success", Group.URL);
    }

    @Test
    public void urlParserDefaultPort() {
        LOG.info("{} group  urlParserDefaultPort test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql://192.168.0.102/army_test/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertEquals(parser.getDbName(), "army_test", postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");


        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);

        assertEquals(host.get(PgKey.PGHOST.getKey()), "192.168.0.102", "host");
        assertNull(host.get(PgKey.PGPORT.getKey()), "port");

        LOG.info("{} group  urlParserDefaultPort test success", Group.URL);
    }

    @Test
    public void urlParserPortNoDatabase() {
        LOG.info("{} group  urlParserPortNoDatabase test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql://192.168.0.102/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertNull(parser.getDbName(), postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");


        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);

        assertEquals(host.get(PgKey.PGHOST.getKey()), "192.168.0.102", "host");
        assertNull(host.get(PgKey.PGPORT.getKey()), "port");

        LOG.info("{} group  urlParserPortNoDatabase test success", Group.URL);
    }

    @Test
    public void urlParserDefaultAll() {
        LOG.info("{} group  urlParser5 test start", Group.URL);
        final String url = "jdbc:postgresql:/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(url, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), url, url);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, url);
        assertNull(parser.getSubProtocol(), url);
        assertNull(parser.getDbName(), url);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");

        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 1, "hostList size");

        final Map<String, String> host = hostList.get(0);

        assertNull(host.get(PgKey.PGHOST.getKey()), "host");
        assertNull(host.get(PgKey.PGPORT.getKey()), "port");

        LOG.info("{} group  urlParser5 test success", Group.URL);
    }


    @Test
    public void failOverHostList() {
        LOG.info("{} group  failOverHostList test start", Group.URL);
        final String url = "jdbc:postgresql://192.168.0.102,[2001:DB8:0:23:8:800:200C:417A],localhost:7878,[2002:DB8:0:23:8:233:200C:417A]:5656/army_test/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(url, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), url, url);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, url);
        assertNull(parser.getSubProtocol(), url);
        assertEquals(parser.getDbName(), "army_test", url);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get("ar"), "3", "ar");

        final List<Map<String, String>> hostList = parser.getHostInfo();

        assertNotNull(hostList, "hostList");
        assertEquals(hostList.size(), 4, "hostList size");

        final Map<String, String> host1 = hostList.get(0);

        assertEquals(host1.get(PgKey.PGHOST.getKey()), "192.168.0.102", "host1");
        assertNull(host1.get(PgKey.PGPORT.getKey()), "port1");

        final Map<String, String> host2 = hostList.get(1);

        assertEquals(host2.get(PgKey.PGHOST.getKey()), "2001:DB8:0:23:8:800:200C:417A", "host2");
        assertNull(host2.get(PgKey.PGPORT.getKey()), "port2");

        final Map<String, String> host3 = hostList.get(2);

        assertEquals(host3.get(PgKey.PGHOST.getKey()), "localhost", "host3");
        assertEquals(host3.get(PgKey.PGPORT.getKey()), "7878", "port3");

        final Map<String, String> host4 = hostList.get(3);

        assertEquals(host4.get(PgKey.PGHOST.getKey()), "2002:DB8:0:23:8:233:200C:417A", "host4");
        assertEquals(host4.get(PgKey.PGPORT.getKey()), "5656", "port4");

        LOG.info("{} group  failOverHostList test success", Group.URL);
    }


    @Test(expectedExceptions = UrlException.class)
    public void urlParserError1() {
        String url = "jdbc:mysql:";
        PostgreUrlParser.create(url, Collections.emptyMap());
    }

    @Test(expectedExceptions = UrlException.class)
    public void urlParserError2() {
        final String url = "jdbc:postgresql:////army_test?ar=3";
        PostgreUrlParser.create(url, Collections.emptyMap());
    }


    @Test(expectedExceptions = UrlException.class)
    public void urlParserError3() {
        final String url = "jdbc:postgresql://user:passwrord@localhost?ar=3";
        PostgreUrlParser.create(url, Collections.emptyMap());
    }


    @Test(expectedExceptions = UrlException.class)
    public void urlParserError4() {
        final String url = "jdbc:postgresql://localhost:port?ar=3";
        PostgreUrlParser.create(url, Collections.emptyMap());
    }


    @Test(expectedExceptions = UrlException.class)
    public void urlParserError5() {
        final String url = "jdbc:postgresql://2001:DB8:0:23:8:800:200C:417A:5432/army_test/?ar=3";
        PostgreUrlParser.create(url, Collections.emptyMap());
    }


}
