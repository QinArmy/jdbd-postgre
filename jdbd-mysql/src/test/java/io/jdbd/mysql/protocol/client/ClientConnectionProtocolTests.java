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

public class ClientConnectionProtocolTests {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolTests.class);

    @BeforeClass
    public static void createConnectionProtocol(ITestContext context) {
        String url = "jdbc:mysql://localhost:3306/army?sslMode=DISABLED";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        ClientConnectionProtocol protocol = ClientConnectionProtocolImpl
                .getInstance(MySQLUrl.getInstance(url, properties))
                .block();

        context.setAttribute("clientConnectionProtocol", protocol);
    }

    @Test
    public void receiveHandshake(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .receiveHandshake()
                .block();
        LOG.info("handshake packet:\n{}", mySQLPacket);
    }

    @Test(dependsOnMethods = "receiveHandshake")
    public void sslRequest(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .ssl()
                .block();

        LOG.info("response ssl packet:\n{}", mySQLPacket);
    }

    @Test(dependsOnMethods = {"sslRequest", "receiveHandshake"})
    public void responseHandshake(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .responseHandshake()
                .block();
        LOG.info("response response packet:\n{}", mySQLPacket);
    }


    /*################################## blow private static method ##################################*/

    private static ClientConnectionProtocol obtainConnectionProtocol(ITestContext context) {
        ClientConnectionProtocol protocol = (ClientConnectionProtocol) context.getAttribute("clientConnectionProtocol");
        Assert.assertNotNull(protocol, "no ClientConnectionProtocol");
        return protocol;
    }
}
