package io.jdbd.mysql.protocol.client;

import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.Test;

@Test(groups = "ClientProtocolGroup")
public class ClientProtocolTests {

    public static final String SUIT = "ClientProtocolSuit";

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
