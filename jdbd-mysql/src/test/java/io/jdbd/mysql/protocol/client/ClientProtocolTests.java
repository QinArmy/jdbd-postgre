package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class ClientProtocolTests {


    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolTests.class);

    @BeforeClass
    public static void createConnectionProtocol(ITestContext context) {
        String url = "jdbc:mysql://localhost:3306/army?sslMode=DISABLED";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        ClientConnectionProtocol protocol = ClientConnectionProtocolImpl
                .from(MySQLUrl.getInstance(url, properties))
                .block();

        context.setAttribute("clientConnectionProtocol", protocol);
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
    public void sslNegotiate(ITestContext context) {
        obtainConnectionProtocol(context)
                .sslNegotiate()
                .block();
        LOG.info("Connection protocol sslNegotiate success");
    }

    /**
     * test {@link ClientConnectionProtocolImpl#authenticate() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate"})
    public void authenticate(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .authenticate()
                .block();
        Assert.assertNotNull(mySQLPacket);
        LOG.info("Connection protocol authenticate phase execute success.packet:\n{}", mySQLPacket);
    }

    /**
     * test {@link ClientConnectionProtocolImpl#configureSessionPropertyGroup() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate"})
    public void configureSessionPropertyGroup(ITestContext context) {
        obtainConnectionProtocol(context)
                .configureSessionPropertyGroup()
                .block();
        LOG.info("Connection protocol configureSessionPropertyGroup phase execute success");
    }

    /**
     * test {@link ClientConnectionProtocolImpl#initialize() }
     */
    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate", "configureSessionPropertyGroup"})
    public void initialize(ITestContext context) {
        obtainConnectionProtocol(context)
                .initialize()
                .block();
        LOG.info("Connection protocol initialize phase execute success");
    }


    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate", "configureSessionPropertyGroup", "initialize"})
    public void comQueryForResultSet(ITestContext context) {
        ClientCommandProtocol commandProtocol;
        commandProtocol = obtainCommandProtocol(context);
        Object result = commandProtocol.comQueryForResultSet("SELECT NOW()")
                .block();
        LOG.info("comQueryForResultSet :{}", result);
    }


    /*################################## blow private static method ##################################*/


    private static void createCommandProtocol(ITestContext context) {
        ClientConnectionProtocolImpl connectionProtocol = obtainConnectionProtocol(context);
        //LOG.info("connectionProtocol:{}",connectionProtocol);
        ClientCommandProtocol commandProtocol;
        commandProtocol = ClientCommandProtocolImpl.getInstance(connectionProtocol)
                .block();
        context.setAttribute("commandProtocol", commandProtocol);
    }

    static ClientConnectionProtocolImpl obtainConnectionProtocol(ITestContext context) {
        ClientConnectionProtocolImpl protocol;
        protocol = (ClientConnectionProtocolImpl) context.getAttribute("clientConnectionProtocol");
        Assert.assertNotNull(protocol, "no ClientConnectionProtocol");
        return protocol;
    }

    static ClientCommandProtocolImpl obtainCommandProtocol(ITestContext context) {
        ClientCommandProtocolImpl protocol;
        protocol = (ClientCommandProtocolImpl) context.getAttribute("commandProtocol");
        Assert.assertNotNull(protocol, "no ClientCommandProtocol");
        return protocol;
    }
}
