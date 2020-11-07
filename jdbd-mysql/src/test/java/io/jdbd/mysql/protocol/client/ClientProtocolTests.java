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
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    public void receiveHandshake(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .receiveHandshake()
                .block();
        Assert.assertNotNull(mySQLPacket);
        LOG.info("handshake packet:\n{}", mySQLPacket.toString());
    }

    @Test(dependsOnMethods = "receiveHandshake")
    public void sslRequest(ITestContext context) {
        MySQLPacket mySQLPacket = obtainConnectionProtocol(context)
                .sslNegotiate()
                .block();

        LOG.info("response ssl packet:\n{}", mySQLPacket);
    }

    @Test(dependsOnMethods = {"sslRequest", "receiveHandshake"})
    public void responseHandshake(ITestContext context) throws Throwable {
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        obtainConnectionProtocol(context)
                .authenticateAndInitializing()
                .doOnSuccess(v -> createCommandProtocol(context))
                .doOnError(error::set)
                .block();
        Throwable e = error.get();
        if (e != null) {
            throw e;
        }
    }


    @Test(dependsOnMethods = {"sslRequest", "receiveHandshake", "responseHandshake"})
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
