package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.TestConstant;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = {TestConstant.AUTHENTICATE_PLUGIN_GROUP}, dependsOnGroups = {TestConstant.MYSQL_URL_GROUP})
public class AuthenticatePluginTests {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatePluginTests.class);

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n{} group test start.\n", TestConstant.AUTHENTICATE_PLUGIN_GROUP);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n{} group test end.\n", TestConstant.AUTHENTICATE_PLUGIN_GROUP);
    }


    @Test
    public void defaultPlugin() {
        LOG.info("defaultPlugin test start.");
        Map<String, String> propMap;
        propMap = new HashMap<>();
        // propMap = Collections.singletonMap(PropertyKey.detectCustomCollations.getKey(), "true");
        propMap.put(PropertyKey.detectCustomCollations.getKey(), "true");
        propMap.put(PropertyKey.sslMode.getKey(), "DISABLED");

        MySQLUrl url = ClientTestUtils.singleUrl(propMap);

        ClientConnectionProtocolImpl.create(url.getPrimaryHost(), ClientTestUtils.EVENT_LOOP_GROUP)
                .doOnSuccess(c -> {
                    Assert.assertNotNull(c, "client connection protocol.");
                    LOG.info("defaultPlugin test success. {}", c.taskExecutor.getAdjutant().obtainHandshakeV10Packet());
                })
                .block();


    }

    @Test
    public void abd() {

    }


}
