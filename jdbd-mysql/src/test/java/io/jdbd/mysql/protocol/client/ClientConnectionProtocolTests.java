package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.PropertyDefinitions;
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

        HostInfo hostInfo = MySQLUrlUtils.build(properties).getHosts().get(0);
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

    /**
     * test {@link ClientConnectionProtocolImpl#receiveHandshake() }
     */
    @Test
    public void receiveHandshake(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .receiveHandshake()
                .block();
        Assert.assertNotNull(mySQLPacket);
        LOG.info("Connection protocol receiveHandshake phase success. packet:\n{}", mySQLPacket.toString());
    }

    /**
     * test {@link ClientConnectionProtocolImpl#sslNegotiate() }
     */
    @Test(dependsOnMethods = "receiveHandshake")
    public void sslNegotiate(ITestContext context) throws Exception {
        PropertyDefinitions.SslMode sslMode = obtainConnectionProtocol(context)
                .sslNegotiate()
                .block();
        //Thread.sleep(5000L);
        LOG.info("Connection protocol sslNegotiate {}", sslMode);
    }

    /**
     * test {@link ClientConnectionProtocolImpl#authenticate() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate"})
    public void authenticate(ITestContext context) {
        obtainConnectionProtocol(context)
                .authenticate()
                .block();
        LOG.info("Connection protocol authenticate phase execute success");
    }

    /**
     * test {@link ClientConnectionProtocolImpl#configureSession() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate"})
    public void configureSession(ITestContext context) {
        obtainConnectionProtocol(context)
                .configureSession()
                .block();
        LOG.info("Connection protocol configureSession phase execute success");
    }

    /**
     * test {@link ClientConnectionProtocolImpl#initialize() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate", "configureSession"})
    public void initialize(ITestContext context) {
        obtainConnectionProtocol(context)
                .initialize()
                .block();
        LOG.info("Connection protocol initialize phase execute success");
    }



    /*################################## blow private static method ##################################*/


    static ClientConnectionProtocolImpl obtainConnectionProtocol(ITestContext context) {
        ClientConnectionProtocolImpl protocol;
        protocol = (ClientConnectionProtocolImpl) context.getAttribute("clientConnectionProtocol");
        Assert.assertNotNull(protocol, "no ClientConnectionProtocol");
        return protocol;
    }


}
