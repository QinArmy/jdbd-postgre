package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.*;
import reactor.netty.resources.LoopResources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(groups = {Groups.AUTHENTICATE_PLUGIN}, dependsOnGroups = {Groups.MYSQL_URL, Groups.SQL_PARSER})
public class AuthenticatePluginSuiteTests extends AbstractConnectionBasedSuiteTests {

    static final Logger LOG = LoggerFactory.getLogger(AuthenticatePluginSuiteTests.class);

    private static final long TIME_OUT = 5L * 1000L;

    @BeforeSuite
    public static void beforeSuite(ITestContext context) {
        LOG.info("LoopResources {}", LoopResources.hasNativeSupport());
    }

    @AfterSuite
    public static void afterSuite(ITestContext context) {

    }

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n{} group test start.\n", Groups.AUTHENTICATE_PLUGIN);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n{} group test end.\n", Groups.AUTHENTICATE_PLUGIN);
    }


    @Test(timeOut = TIME_OUT)
    public void defaultPlugin() throws Exception {
        LOG.info("defaultPlugin test start.");
        final Map<String, String> propMap;

        propMap = new HashMap<>();
        propMap.put(PropertyKey.detectCustomCollations.getKey(), "true");
        //propMap.put(PropertyKey.sslMode.getKey(),  Enums.SslMode.PREFERRED.name());

        MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(propMap);

        AuthenticateResult result = MySQLTaskExecutor.create(0, sessionAdjutant)
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.getAdjutant()))
                .block();

        assertNotNull(result, "result");

        HandshakeV10Packet packet = result.handshakeV10Packet();
        assertNotNull(packet, "HandshakeV10Packet");

        assertTrue(result.negotiatedCapability() != 0, "negotiatedCapability");

        LOG.info("defaultPlugin test success. {}", packet);
    }


    @Test(dependsOnMethods = "defaultPlugin", timeOut = TIME_OUT)
    public void defaultPluginWithSslDisabled() {
        LOG.info("defaultPluginWithSslDisabled test start.");
        final Map<String, String> propMap;
        propMap = Collections.singletonMap(PropertyKey.sslMode.getKey()
                , Enums.SslMode.DISABLED.name());

        MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(propMap);

        AuthenticateResult result = MySQLTaskExecutor.create(0, sessionAdjutant)
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.getAdjutant()))
                .block();

        assertNotNull(result, "result");
        LOG.info("defaultPluginWithSslDisabled test success.handshakeV10Packet:\n {}", result.handshakeV10Packet());

    }


}
