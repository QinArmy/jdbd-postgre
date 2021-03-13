package io.jdbd.mysql.protocol.client;


import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.util.MySQLTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.*;


/**
 * @see ComQueryTask
 */
@Test(groups = {Groups.COM_QUERY}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
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
        List<String> commandList = new ArrayList<>(2);
        commandList.add(builder.toString());
        commandList.add("TRUNCATE u_user");

        ComQueryTask.batchUpdate(commandList, taskAdjutant)
                .then()
                .block();

        for (int i = 0; i < 2; i++) {
            prepareData(taskAdjutant);
        }

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
//        protocol.closeGracefully()
//                .block();
    }


    @Test
    public void update(ITestContext context) {
        LOG.info("update test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant(context);

        final String newName = "simonyi";
        String sql = "UPDATE u_user as u SET u.name = '%s' WHERE u.id = 1";
        ResultStates resultStates = ComQueryTask.update(String.format(sql, newName), adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "insertedId");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResult");

        sql = "SELECT u.id,u.name FROM u_user as u WHERE u.id = 1";
        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 1, "resultRowList size");

        ResultRow resultRow = resultRowList.get(0);

        assertEquals(resultRow.getNonNull("id", Long.class), (Object) 1L, "id");
        assertEquals(resultRow.getNonNull("name", String.class), newName, "name");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResult");

        LOG.info("update test success");

    }

    @Test(dependsOnMethods = "update")
    public void delete(ITestContext context) {
        LOG.info("delete test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant(context);
        String sql = "DELETE FROM u_user WHERE u_user.id = 1";

        ResultStates resultStates = ComQueryTask.update(sql, adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "inserted");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResults");

        sql = "SELECT u.id,u.name FROM u_user as u WHERE u.id = 1";

        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertTrue(resultRowList.isEmpty(), "resultRowList is empty");

        LOG.info("delete test success");
    }

    @Test
    public void query(ITestContext context) {
        LOG.info("query test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant(context);
        String sql = "SELECT u.* FROM u_user as u ORDER BY u.id DESC LIMIT 10";
        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertFalse(resultRowList.isEmpty(), "resultRowList is empty");

        for (ResultRow resultRow : resultRowList) {

            resultRow.get("id", Long.class);
            resultRow.get("name", String.class);
            resultRow.get("nick_name", String.class);
            resultRow.get("balance", BigDecimal.class);

            resultRow.get("height", Integer.class);
            resultRow.get("wake_up_time", LocalTime.class);
            resultRow.get("wake_up_time", OffsetTime.class);
            resultRow.get("wake_up_time", String.class);

            resultRow.get("create_time", LocalDateTime.class);
            resultRow.get("create_time", LocalDate.class);
            resultRow.get("create_time", LocalTime.class);
            resultRow.get("create_time", ZonedDateTime.class);

            resultRow.get("create_time", OffsetDateTime.class);
            resultRow.get("create_time", OffsetTime.class);
            resultRow.get("create_time", Year.class);
            resultRow.get("create_time", YearMonth.class);

            resultRow.get("create_time", MonthDay.class);
            resultRow.get("create_time", Month.class);
            resultRow.get("create_time", DayOfWeek.class);
            resultRow.get("create_time", String.class);

            resultRow.get("create_time", Instant.class);
            resultRow.get("update_time", LocalDateTime.class);
            resultRow.get("birthday", LocalDate.class);

            assertNull(resultRow.get("love_music"), "love_music");

        }

        LOG.info("query test success");
    }



    /*################################## blow private method ##################################*/


    private MySQLTaskAdjutant obtainTaskAdjutant(ITestContext context) {
        ClientConnectionProtocolImpl protocol = (ClientConnectionProtocolImpl) context.getAttribute(PROTOCOL_KEY);
        assertNotNull(protocol, "protocol");
        return protocol.taskExecutor.getAdjutant();
    }

    private static void prepareData(MySQLTaskAdjutant taskAdjutant) {
        final int rowCount = 1000;

        StringBuilder builder = new StringBuilder(40 * rowCount)
                .append("INSERT INTO u_user(name,nick_name,balance,birthday,height,wake_up_time) VALUES");

        final LocalDate age18Birthday = LocalDate.now().minusYears(18);
        final int ageBound = 365 * 80;
        final int genericHeight = 166;
        final Random random = new Random();
        final LocalTime wakeUpTime = LocalTime.of(6, 0, 0);
        final int minutes = 60 * 8;

        for (int i = 1, height; i <= rowCount; i++) {
            if (i > 1) {
                builder.append(",");
            }
            height = random.nextInt(20);
            if ((height & 1) == 0) {
                height += genericHeight;
            } else {
                height = genericHeight - height;
            }
            builder.append("(")
                    //.append(i)//id
                    .append("'zoro")//name
                    .append(i)
                    .append("','simonyi")//nickname
                    .append(i)
                    .append("',")//balance
                    .append(random.nextInt(Integer.MAX_VALUE))
                    .append(".")
                    .append(random.nextInt(100))
                    .append(",'")
                    .append(age18Birthday.minusDays(random.nextInt(ageBound)))//birthday
                    .append("',")
                    .append(height)//height
                    .append(",'")//wake_up_time
                    .append(wakeUpTime.plusMinutes(random.nextInt(minutes)).format(MySQLTimeUtils.MYSQL_TIME_FORMATTER))
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
