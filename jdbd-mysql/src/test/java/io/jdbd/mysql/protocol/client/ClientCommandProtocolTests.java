package io.jdbd.mysql.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

@Test(groups = {ClientCommandProtocolTests.GROUP}, dependsOnGroups = {ClientConnectionProtocolTests.GROUP})
public class ClientCommandProtocolTests {

    public static final String GROUP = "MySQLCommandGroup";

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolTests.class);


    @BeforeGroups(groups = {GROUP}, dependsOnGroups = {ClientConnectionProtocolTests.GROUP})
    public void createCommandProtocol(ITestContext context) {
        ClientConnectionProtocolImpl connectionProtocol = ClientProtocolTests.obtainConnectionProtocol(context);
        //LOG.info("connectionProtocol:{}",connectionProtocol);
        ClientCommandProtocol commandProtocol;
        commandProtocol = ClientCommandProtocolImpl.getInstance(connectionProtocol)
                .block();
        context.setAttribute("commandProtocol", commandProtocol);
    }

    @Test(groups = {GROUP})
    public void comQueryForResultSet(ITestContext context) {
        ClientCommandProtocol commandProtocol;
        commandProtocol = ClientProtocolTests.obtainCommandProtocol(context);
        Object result = commandProtocol.comQueryForResultSet("SELECT NOW()")
                .block();
        LOG.info("comQueryForResultSet :{}", result);
    }


}
