package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.ResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.Assert.*;

@Test//(groups = {Groups.SESSION_INITIALIZER}, dependsOnGroups = {Groups.AUTHENTICATE_PLUGIN})
public class SessionInitializerSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionInitializerSuiteTests.class);

    private static final ConcurrentMap<Long, ClientConnectionProtocol> protocolMap = new ConcurrentHashMap<>();

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n {} group test start.\n", Groups.SESSION_INITIALIZER);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n {} group test end.\n", Groups.SESSION_INITIALIZER);
        LOG.info("close {} ,size:{}", ClientConnectionProtocol.class.getName(), protocolMap.size());

        Flux.fromIterable(protocolMap.values())
                .flatMap(ClientConnectionProtocol::closeGracefully)
                .then()
                .block();

        protocolMap.clear();
    }


    @Test(timeOut = TIME_OUT)
    public void connectAndInitializing() {
        LOG.info("connectAndInitializing test start.");
        doConnectionTest(Collections.singletonMap(PropertyKey.sslMode.getKey(), "DISABLED"));

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
        TaskAdjutant adjutant;

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
        assertEquals(zoneOffsetClient, MySQLTimes.systemZoneOffset(), "zoneOffsetClient");

        propMap.put(PropertyKey.connectionTimeZone.getKey(), "+03:17");
        adjutant = doConnectionTest(propMap);
        zoneOffsetClient = adjutant.obtainZoneOffsetClient();
        assertEquals(zoneOffsetClient, ZoneOffset.of("+03:17"), "zoneOffsetClient");

        propMap.put(PropertyKey.connectionTimeZone.getKey(), "Australia/Sydney");
        adjutant = doConnectionTest(propMap);
        zoneOffsetClient = adjutant.obtainZoneOffsetClient();
        assertEquals(zoneOffsetClient, MySQLTimes.toZoneOffset(ZoneOffset.of("Australia/Sydney", ZoneOffset.SHORT_IDS)), "zoneOffsetClient");

        LOG.info("configConnectionZone test success.");

    }


    /**
     * @see DefaultSessionResetter#configSessionCharset()
     */
    @Test
    public void configSessionCharsets() {
        LOG.info("configSessionCharsets test start.");
        TaskAdjutant adjutant;

        final Map<String, String> propMap;
        propMap = new HashMap<>();

        propMap.put(PropertyKey.characterSetResults.getKey(), "GBK");
        adjutant = doConnectionTest(propMap);
        assertEquals(adjutant.getCharsetResults(), Charset.forName("GBK"));

        propMap.remove(PropertyKey.characterSetResults.getKey());
        adjutant = doConnectionTest(propMap);
        assertNull(adjutant.getCharsetResults(), "charset results");

        propMap.put(PropertyKey.characterEncoding.getKey(), StandardCharsets.UTF_8.name());
        adjutant = doConnectionTest(propMap);
        assertNull(adjutant.getCharsetResults(), "charset results");
        assertEquals(adjutant.obtainCharsetClient(), StandardCharsets.UTF_8, "charset client");


        propMap.put(PropertyKey.connectionCollation.getKey(), "utf8mb4");
        propMap.remove(PropertyKey.characterSetResults.getKey());
        adjutant = doConnectionTest(propMap);
        assertNull(adjutant.getCharsetResults(), "charset results");
        assertEquals(adjutant.obtainCharsetClient(), StandardCharsets.UTF_8, "charset client");
        String sql = "SELECT @@character_set_connection as  characterSetConnection" +
                ", @@character_set_results as characterSetResults," +
                "@@character_set_client as characterSetClient";
        ResultRow resultRow = ComQueryTask.query(Stmts.stmt(sql), adjutant)
                .elementAt(0)
                .block();
        assertNotNull(resultRow);

        assertEquals(resultRow.get("characterSetConnection", String.class), "utf8mb4", "characterSetConnection");
        assertEquals(resultRow.get("characterSetClient", String.class), "utf8mb4", "characterSetClient");
        assertNull(resultRow.get("characterSetResults", String.class), "characterSetResults");

        LOG.info("configSessionCharsets test success.");
    }


    @Test
    public void configSqlMode() {
        LOG.info("configSqlMode test start.");
        TaskAdjutant adjutant;

        final Map<String, String> propMap;
        propMap = new HashMap<>();

        propMap.put(PropertyKey.timeTruncateFractional.getKey(), "true");
        adjutant = doConnectionTest(propMap);
        Server server = adjutant.obtainServer();

        assertNotNull(server, "server");
        assertTrue(server.containSqlMode(SQLMode.TIME_TRUNCATE_FRACTIONAL), "TIME_TRUNCATE_FRACTIONAL");

        LOG.info("configSqlMode test success.");

    }



    /*################################## blow private method ##################################*/

    private TaskAdjutant doConnectionTest(Map<String, String> propMap) {
        SessionAdjutant sessionAdjutant = createSessionAdjutantForSingleHost(propMap);

        ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();

        assertNotNull(protocol, "protocol");
        assertNotNull(protocol.taskExecutor, "protocol.taskExecutor");
        assertNotNull(protocol.sessionResetter, "protocol.sessionResetter");

        TaskAdjutant adjutant = protocol.taskExecutor.taskAdjutant();
        protocolMap.put(adjutant.obtainHandshakeV10Packet().getThreadId(), protocol);
        return adjutant;
    }


}
