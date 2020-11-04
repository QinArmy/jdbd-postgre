package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = {ClientConnectionProtocolTests.GROUP})
public class ClientConnectionProtocolTests {

    public static final String GROUP = "MySQLConnectionGroup";

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolTests.class);

    @BeforeGroups(groups = GROUP)
    public void createConnectionProtocol(ITestContext context) {
        String url = "jdbc:mysql://localhost:3306/army?sslMode=DISABLED";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        ClientConnectionProtocol protocol = ClientConnectionProtocolImpl
                .getInstance(MySQLUrl.getInstance(url, properties))
                .block();

        context.setAttribute("clientConnectionProtocol", protocol);
    }

    //@AfterGroups(groups = GROUP)
    public void printProtocol(ITestContext context) {
        ClientConnectionProtocol protocol = ClientProtocolTests.obtainConnectionProtocol(context);
        LOG.info("ClientConnectionProtocol:{}", protocol);
    }

    @Test
    public void receiveHandshake(ITestContext context) {
        MySQLPacket mySQLPacket = ClientProtocolTests.obtainConnectionProtocol(context)
                .receiveHandshake()
                .block();
        LOG.info("handshake packet:\n{}", mySQLPacket);
    }

    @Test(dependsOnMethods = "receiveHandshake")
    public void sslRequest(ITestContext context) {
        MySQLPacket mySQLPacket = ClientProtocolTests.obtainConnectionProtocol(context)
                .ssl()
                .block();

        LOG.info("response ssl packet:\n{}", mySQLPacket);
    }

    @Test(dependsOnMethods = {"sslRequest", "receiveHandshake"})
    public void responseHandshake(ITestContext context) {
        ClientProtocolTests.obtainConnectionProtocol(context)
                .responseHandshakeAndAuthenticate()
                .doOnSuccess(v -> LOG.info("authentication success."))
                .doOnError(e -> LOG.error("authentication failure.", e))
                .block();
    }


    /*################################## blow private static method ##################################*/


}
