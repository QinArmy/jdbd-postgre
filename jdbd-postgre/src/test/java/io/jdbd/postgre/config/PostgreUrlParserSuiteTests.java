package io.jdbd.postgre.config;


import io.jdbd.config.UrlException;
import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
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
        assertNull(globalMap.get(Property.PGHOST.getKey()), "host");
        assertNull(globalMap.get(Property.PGPORT.getKey()), "port");
        assertEquals(globalMap.get(Property.PGDBNAME.getKey()), "army_test", "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

        LOG.info("{} group  urlParserDefaultHostAndPort test success", Group.URL);
    }


    @Test
    public void urlParserIpv6() {
        LOG.info("{} group  urlParserIpv6 test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql://[2001:DB8:0:23:8:800:200C:417A]:5432/army_test/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertEquals(parser.getDbName(), "army_test", postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertEquals(globalMap.get(Property.PGHOST.getKey()), "2001:DB8:0:23:8:800:200C:417A", "host");
        assertEquals(globalMap.get(Property.PGPORT.getKey()), "5432", "port");
        assertEquals(globalMap.get(Property.PGDBNAME.getKey()), "army_test", "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

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
        assertEquals(globalMap.get(Property.PGHOST.getKey()), "192.168.0.102", "host");
        assertEquals(globalMap.get(Property.PGPORT.getKey()), "5432", "port");
        assertEquals(globalMap.get(Property.PGDBNAME.getKey()), "army_test", "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

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
        assertEquals(globalMap.get(Property.PGHOST.getKey()), "192.168.0.102", "host");
        assertNull(globalMap.get(Property.PGPORT.getKey()), "port");
        assertEquals(globalMap.get(Property.PGDBNAME.getKey()), "army_test", "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

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
        assertEquals(globalMap.get(Property.PGHOST.getKey()), "192.168.0.102", "host");
        assertNull(globalMap.get(Property.PGPORT.getKey()), "port");
        assertNull(globalMap.get(Property.PGDBNAME.getKey()), "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

        LOG.info("{} group  urlParserPortNoDatabase test success", Group.URL);
    }

    @Test
    public void urlParserDefaultAll() {
        LOG.info("{} group  urlParser5 test start", Group.URL);
        final String postgreUrl = "jdbc:postgresql:/?ar=3";
        final PostgreUrlParser parser;
        parser = PostgreUrlParser.create(postgreUrl, Collections.emptyMap());

        assertEquals(parser.getOriginalUrl(), postgreUrl, postgreUrl);
        assertEquals(parser.getProtocol(), PostgreUrl.PROTOCOL, postgreUrl);
        assertNull(parser.getSubProtocol(), postgreUrl);
        assertNull(parser.getDbName(), postgreUrl);

        final Map<String, String> globalMap = parser.getGlobalProperties();
        assertNotNull(globalMap, "globalMap");
        assertFalse(globalMap.isEmpty(), "globalMap");
        assertNull(globalMap.get(Property.PGHOST.getKey()), "host");
        assertNull(globalMap.get(Property.PGPORT.getKey()), "port");
        assertNull(globalMap.get(Property.PGDBNAME.getKey()), "database");

        assertEquals(globalMap.get("ar"), "3", "ar");

        LOG.info("{} group  urlParser5 test success", Group.URL);
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
