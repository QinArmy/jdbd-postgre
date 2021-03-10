package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.util.MySQLTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

@Test(groups = {Groups.CONNECTION_PHASE}, dependsOnGroups = {Groups.AUTHENTICATE_PLUGIN})
public class SessionInitializerSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionInitializerSuiteTests.class);

    @Test(timeOut = TIME_OUT)
    public void connectAndInitializing() {
        LOG.info("connectAndInitializing test start.");
        doConnectionTest(Collections.emptyMap());

        LOG.info("connectAndInitializing test success.");

    }

    @Test(dependsOnMethods = "connectAndInitializing", timeOut = TIME_OUT)
    public void detectCustomCollation() {
        LOG.info("detectCustomCollation test start.");
        final Map<String, String> propMap;
        propMap = Collections.singletonMap(PropertyKey.detectCustomCollations.getKey(), "true");
        doConnectionTest(propMap);
        LOG.info("detectCustomCollation test success.");
    }

    /**
     * @see SessionResetter#reset()
     */
    @Test(dependsOnMethods = "connectAndInitializing", timeOut = TIME_OUT)
    public void sessionResetter() {
        LOG.info("sessionResetter test start.");
        final Map<String, String> propMap;
        propMap = new HashMap<>();

        propMap.put(PropertyKey.sessionVariables.getKey(), "autocommit=1, transaction_isolation='REPEATABLE-READ'");

        doConnectionTest(propMap);
        LOG.info("sessionResetter test success.");
    }

    /**
     * @see DefaultSessionResetter#configZoneOffsets()
     */
    @Test
    public void configConnectionZone() {
        LOG.info("configConnectionZone test start.");
        MySQLTaskAdjutant adjutant;

        final Map<String, String> propMap;
        propMap = new HashMap<>();

        propMap.put(PropertyKey.connectionTimeZone.getKey(), "SERVER");
        propMap.put(PropertyKey.sessionVariables.getKey(), "time_zone='+04:14'");
        adjutant = doConnectionTest(propMap);

        ZoneOffset zoneOffset = ZoneOffset.of("+04:14");
        ZoneOffset zoneOffsetDatabase = adjutant.obtainZoneOffsetDatabase();
        ZoneOffset zoneOffsetClient = adjutant.obtainZoneOffsetClient();

        assertEquals(zoneOffsetClient, zoneOffsetDatabase, "zoneOffsetClient");
        assertEquals(zoneOffsetDatabase, zoneOffset, "zoneOffsetDatabase");


        propMap.put(PropertyKey.connectionTimeZone.getKey(), "LOCAL");
        adjutant = doConnectionTest(propMap);
        zoneOffsetClient = adjutant.obtainZoneOffsetClient();
        assertEquals(zoneOffsetClient, MySQLTimeUtils.systemZoneOffset(), "zoneOffsetClient");

        propMap.put(PropertyKey.connectionTimeZone.getKey(), "+03:17");
        adjutant = doConnectionTest(propMap);
        zoneOffsetClient = adjutant.obtainZoneOffsetClient();
        assertEquals(zoneOffsetClient, ZoneOffset.of("+03:17"), "zoneOffsetClient");

        propMap.put(PropertyKey.connectionTimeZone.getKey(), "Australia/Sydney");
        adjutant = doConnectionTest(propMap);
        zoneOffsetClient = adjutant.obtainZoneOffsetClient();
        assertEquals(zoneOffsetClient, MySQLTimeUtils.toZoneOffset(ZoneOffset.of("Australia/Sydney", ZoneOffset.SHORT_IDS)), "zoneOffsetClient");

        LOG.info("configConnectionZone test success.");

    }

    /**
     * @see DefaultSessionResetter#configSessionCharset()
     */
    @Test
    public void configSessionCharsets() {
        LOG.info("configSessionCharsets test start.");
        MySQLTaskAdjutant adjutant;

        final Map<String, String> propMap;
        propMap = new HashMap<>();

        propMap.put(PropertyKey.characterSetResults.getKey(), "GBK");
        adjutant = doConnectionTest(propMap);
        assertEquals(adjutant.getCharsetResults(), Charset.forName("GBK"));

        propMap.remove(PropertyKey.characterSetResults.getKey());
        adjutant = doConnectionTest(propMap);
        assertNull(adjutant.getCharsetResults(), "charset results");

        LOG.info("configSessionCharsets test success.");
    }



    /*################################## blow private method ##################################*/

    private MySQLTaskAdjutant doConnectionTest(Map<String, String> propMap) {
        MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(propMap);

        ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();

        assertNotNull(protocol, "protocol");
        assertNotNull(protocol.taskExecutor, "protocol.taskExecutor");
        assertNotNull(protocol.sessionResetter, "protocol.sessionResetter");

        return protocol.taskExecutor.getAdjutant();
    }


}
