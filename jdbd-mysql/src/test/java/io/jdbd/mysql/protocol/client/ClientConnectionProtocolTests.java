package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.HostInfo;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.netty.resources.LoopResources;

import java.util.HashMap;
import java.util.Map;

public class ClientConnectionProtocolTests {


    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolTests.class);

    @BeforeClass
    public static void createConnectionProtocol(ITestContext context) {
        // PREFERRED ,DISABLED
        Map<String, String> properties = new HashMap<>();

        properties.put("sslMode", "PREFERRED");

        HostInfo<PropertyKey> hostInfo = MySQLUrlUtils.build(properties).getHostList().get(0);
        EventLoopGroup eventLoopGroup = LoopResources.create("jdbd-mysql").onClient(true);

        ClientConnectionProtocol protocol = ClientConnectionProtocolImpl
                .create(hostInfo, eventLoopGroup)
                .block();

        context.setAttribute("clientConnectionProtocol", protocol);
    }

    @AfterClass
    public void closeClientConnectionProtocol(ITestContext context) {
        obtainConnectionProtocol(context)
                .closeGracefully()
                .block();
    }

    @Test
    public void authenticateAndInitializing(ITestContext context) throws Exception {
        obtainConnectionProtocol(context)
                .authenticateAndInitializing()
                .block();
        Thread.sleep(15 * 1000L);
    }



    /*################################## blow private static method ##################################*/


    static ClientConnectionProtocolImpl obtainConnectionProtocol(ITestContext context) {
        ClientConnectionProtocolImpl protocol;
        protocol = (ClientConnectionProtocolImpl) context.getAttribute("clientConnectionProtocol");
        Assert.assertNotNull(protocol, "no ClientConnectionProtocol");
        return protocol;
    }


}
