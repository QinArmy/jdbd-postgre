package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyDefinitions;
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
        // PREFERRED ,DISABLED
        String url = "jdbc:mysql://localhost:3306/army";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        properties.put("sslMode", "DISABLED");
        properties.put("detectCustomCollations", "true");
        properties.put("sessionVariables", "time_zone='+08:00'");


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


    @Test(dependsOnMethods = {"receiveHandshake", "sslNegotiate", "authenticate", "configureSession", "initialize"})
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
