package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.*;
import reactor.netty.resources.LoopResources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;

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
    public void defaultPlugin() {
        LOG.info("defaultPlugin test start.");
        final Map<String, String> propMap;

        propMap = new HashMap<>();
        propMap.put(PropertyKey.detectCustomCollations.getKey(), "true");
        //propMap.put(PropertyKey.sslMode.getKey(),  Enums.SslMode.PREFERRED.name());

        MySQLUrl url = ClientTestUtils.singleUrl(propMap);

        ClientConnectionProtocolImpl.create(url.getPrimaryHost(), getEventLoopGroup())
                .doOnSuccess(c -> {
                    assertNotNull(c, "client connection protocol.");
                    MySQLTaskAdjutant adjutant = c.taskExecutor.getAdjutant();
                    assertNotNull(adjutant, "adjutant");

                    HandshakeV10Packet packet = adjutant.obtainHandshakeV10Packet();
                    assertNotNull(packet, "HandshakeV10Packet");
                    LOG.info("defaultPlugin test success. {}", packet);
                })
                .flatMap(ClientConnectionProtocol::closeGracefully)
                .block();

    }

    @Test(dependsOnMethods = "defaultPlugin", timeOut = TIME_OUT)
    public void defaultPluginWithSslDisabled() {
        LOG.info("defaultPluginWithSslDisabled test start.");

        MySQLUrl url = ClientTestUtils.singleUrl(Collections.singletonMap(PropertyKey.sslMode.getKey()
                , Enums.SslMode.DISABLED.name()));

        ClientConnectionProtocolImpl.create(url.getPrimaryHost(), getEventLoopGroup())
                .doOnSuccess(c -> LOG.info("defaultPlugin test success. {}"
                        , c.taskExecutor.getAdjutant().obtainHandshakeV10Packet()))
                .block();
    }


}
