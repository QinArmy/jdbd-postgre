package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.ResultStates;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.*;

@Test(groups = {Groups.DATA_PREPARE}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class DataPrepareSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);

    @BeforeSuite
    public static void beforeSuite(ITestContext context) throws Exception {
        LOG.info("\n {} group test start.\n", Groups.DATA_PREPARE);

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

        List<String> commandList = new ArrayList<>(2);
        commandList.add(builder.toString());
        commandList.add("TRUNCATE mysql_types");

        ComQueryTask.batchUpdate(commandList, taskAdjutant)
                .then()
                .block();

        prepareData(taskAdjutant);

        LOG.info("create mysql_types table success");
    }

    @AfterSuite
    public static void afterSuite(ITestContext context) {
        LOG.info("\n {} group test end.\n", Groups.COM_QUERY);
        LOG.info("close {}", ClientConnectionProtocol.class.getName());

        ClientConnectionProtocolImpl protocol = (ClientConnectionProtocolImpl) context.removeAttribute(PROTOCOL_KEY);
        assertNotNull(protocol, "protocol");
        MySQLTaskAdjutant adjutant = protocol.taskExecutor.getAdjutant();

        if (ClientTestUtils.getTestConfig().getProperty("truncate.after.suite", Boolean.class, Boolean.TRUE)) {
            ComQueryTask.update("TRUNCATE mysql_types", adjutant)
                    .then(Mono.defer(protocol::closeGracefully))
                    .block();
        } else {
            protocol.closeGracefully()
                    .block();
        }

        Flux.fromIterable(TASK_ADJUTANT_QUEUE)
                .flatMap(ComQueryTaskSuiteTests::quitConnection)
                .then()
                .block();

        TASK_ADJUTANT_QUEUE.clear();

    }

    @Test
    public void prepare() {
        //no-op
    }

    private static void prepareData(MySQLTaskAdjutant taskAdjutant) throws Exception {
        final int rowCount = 100;

        StringBuilder builder = new StringBuilder(40 * rowCount)
                .append("INSERT INTO mysql_types(name,my_char,my_bit,my_boolean,my_json) VALUES");

        final Random random = new Random();

        final ObjectMapper mapper = new ObjectMapper();

        for (int i = 1; i <= rowCount; i++) {
            if (i > 1) {
                builder.append(",");
            }
            builder.append("(")
                    //.append(i)//id
                    .append("'zoro")//name
                    .append(i)
                    .append("','simonyi")//my_char
                    .append(i)
                    .append("',B'")
                    .append(Long.toBinaryString(random.nextLong()))
                    .append("',TRUE")
                    .append(",'")
                    .append(mapper.writeValueAsString(Collections.singletonMap("name", "zoro")))
                    .append("')");
        }

        final String command = builder.toString();
        // LOG.info("prepare data command:\n {}", builder.toString());
//        byte[] bytes = command.getBytes(taskAdjutant.obtainCharsetClient());
//        LOG.info("prepare data command bytes:{}, times:{}",bytes.length,bytes.length / PacketUtils.MAX_PAYLOAD);
//
        ResultStates resultStates = ComQueryTask.update(command, taskAdjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), rowCount, "affectedRows");
        assertFalse(resultStates.hasMoreResults(), "hasMoreResults");
        LOG.info("InsertId:{}", resultStates.getInsertId());

    }

}
