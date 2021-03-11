package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.Groups;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.testng.Assert.assertNotNull;

@Test(groups = {Groups.COM_QUERY}, dependsOnGroups = {Groups.SESSION_INITIALIZER})
public class ComQueryTaskSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);

    private static final String PROTOCOL_KEY = "my$protocol";

    @BeforeClass
    public static void beforeClass(ITestContext context) throws Exception {
        LOG.info("\n {} group test start.\n", Groups.COM_QUERY);

        MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(Collections.emptyMap());
        ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();
        assertNotNull(protocol, "protocol");
        context.setAttribute(PROTOCOL_KEY, protocol);

        MySQLTaskAdjutant taskAdjutant = protocol.taskExecutor.getAdjutant();

        Path path = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "script/ddl/comQueryTask.sql");
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = Files.newBufferedReader(path, ClientTestUtils.getSystemFileCharset())) {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }

        }

        ComQueryTask.update(builder.toString(), taskAdjutant)
                .then(Mono.defer(() -> ComQueryTask.update("TRUNCATE u_user", taskAdjutant)))
                .then()
                .block();
        LOG.info("create u_user table success");

    }

    @AfterClass
    public static void afterClass(ITestContext context) {
        LOG.info("\n {} group test end.\n", Groups.COM_QUERY);
        LOG.info("close {}", ClientConnectionProtocol.class.getName());

        ClientConnectionProtocolImpl protocol = (ClientConnectionProtocolImpl) context.removeAttribute(PROTOCOL_KEY);
        assertNotNull(protocol, "protocol");
        MySQLTaskAdjutant adjutant = protocol.taskExecutor.getAdjutant();

        ComQueryTask.update("TRUNCATE u_user", adjutant)
                .then(Mono.defer(protocol::closeGracefully))
                .block();
    }

    @Test
    public void prepareData(ITestContext context) {

    }

    /*################################## blow private method ##################################*/

    private MySQLTaskAdjutant obtainTaskAdjutant(ITestContext context) {
        ClientConnectionProtocolImpl protocol = (ClientConnectionProtocolImpl) context.getAttribute(PROTOCOL_KEY);
        assertNotNull(protocol, "protocol");
        return protocol.taskExecutor.getAdjutant();
    }


}
