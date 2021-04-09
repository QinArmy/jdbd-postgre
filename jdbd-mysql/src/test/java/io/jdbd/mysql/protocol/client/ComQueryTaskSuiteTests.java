package io.jdbd.mysql.protocol.client;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.*;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.stmt.StmtWrappers;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.*;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.util.Geometries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.*;


/**
 * @see ComQueryTask
 */
@Test(groups = {Groups.COM_QUERY}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class ComQueryTaskSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);

    private static final String PROTOCOL_KEY = "my$protocol";

    private static final Queue<MySQLTaskAdjutant> TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

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
        commandList.add("TRUNCATE mysql_types");

        ComQueryTask.batchUpdate(commandList, taskAdjutant)
                .then()
                .block();

        prepareData(taskAdjutant);

        LOG.info("create mysql_types table success");
    }

    @AfterClass
    public static void afterClass(ITestContext context) {
        LOG.info("\n {} group test end.\n", Groups.COM_QUERY);
        LOG.info("close {}", ClientConnectionProtocol.class.getName());

        ClientConnectionProtocolImpl protocol = (ClientConnectionProtocolImpl) context.removeAttribute(PROTOCOL_KEY);
        assertNotNull(protocol, "protocol");
        MySQLTaskAdjutant adjutant = protocol.taskExecutor.getAdjutant();

//        ComQueryTask.update("TRUNCATE mysql_types", adjutant)
//                .then(Mono.defer(protocol::closeGracefully))
//                .block();

        protocol.closeGracefully()
                .block();

        Flux.fromIterable(TASK_ADJUTANT_QUEUE)
                .flatMap(ComQueryTaskSuiteTests::quitConnection)
                .then()
                .block();

        TASK_ADJUTANT_QUEUE.clear();

    }


    @Test
    public void update() {
        LOG.info("update test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String newName = "simonyi";
        String sql = "UPDATE mysql_types as u SET u.name = '%s' WHERE u.id = 1";
        ResultStates resultStates = ComQueryTask.update(String.format(sql, newName), adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "insertedId");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResult");

        sql = "SELECT u.id,u.name FROM mysql_types as u WHERE u.id = 1";
        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 1, "resultRowList size");

        ResultRow resultRow = resultRowList.get(0);

        assertEquals(resultRow.getNonNull("id", Long.class), (Object) 1L, "id");
        assertEquals(resultRow.getNonNull("name", String.class), newName, "name");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResult");

        releaseConnection(adjutant);
        LOG.info("update test success");

    }

    @Test(dependsOnMethods = {"update"})
    public void delete() {
        LOG.info("delete test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        String sql = "DELETE FROM mysql_types WHERE mysql_types.id = 1";

        ResultStates resultStates = ComQueryTask.update(sql, adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "inserted");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResults");

        sql = "SELECT u.id,u.name FROM mysql_types as u WHERE u.id = 1";

        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertTrue(resultRowList.isEmpty(), "resultRowList is empty");

        LOG.info("delete test success");
        releaseConnection(adjutant);
    }

    @Test
    public void mysqlTypeMetadataMatch() {
        LOG.info("mysqlTypeMatch test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        List<ResultRow> resultRowList = ComQueryTask.query(createQuerySqlForMySQLTypeMatch(), MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .doOnError(this::printMultiError)
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertFalse(resultRowList.isEmpty(), "resultRowList is empty");

        final Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        for (ResultRow resultRow : resultRowList) {
            assertQueryResultRowMySQLTypeMatch(resultRow, properties);
        }

        LOG.info("mysqlTypeMatch test success");
        releaseConnection(adjutant);
    }

    @Test
    public void bigIntBindAndExtract() {
        LOG.info("bigIntBindAndExtract test start");

        final String sql = "SELECT t.id as id, t.create_time as createTime FROM mysql_types as t WHERE t.id  = ?";
        BindValue bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, 2);
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        List<ResultRow> resultRowList;
        resultRowList = ComQueryTask.bindableQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        Long id = resultRow.get("id", Long.class);

        assertEquals(id, Long.valueOf(2L), "id");

        // string bigint
        bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, "2");
        resultRowList = ComQueryTask.bindableQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        id = resultRow.get("id", Long.class);

        assertEquals(id, Long.valueOf(2L), "id");

        LOG.info("bigIntBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void datetimeBindAndExtract() {
        LOG.info("datetimeBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, "2021-03-16 19:26:00.999999", "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, LocalDateTime.now(), "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, OffsetDateTime.now(ZoneOffset.of("+03:00")), "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, ZonedDateTime.now(ZoneOffset.of("+04:00")), "create_time");

        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, "2021-03-16 19:26:00.999999", "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, LocalDateTime.now(), "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, OffsetDateTime.now(ZoneOffset.of("+03:00")), "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, ZonedDateTime.now(ZoneOffset.of("+04:00")), "update_time");

        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, "2021-03-16 19:26:00.999999", "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, LocalDateTime.parse("2021-03-16 19:26:00.999999", MySQLTimeUtils.MYSQL_DATETIME_FORMATTER), "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, OffsetDateTime.parse("2021-03-16 19:26:00.999999+03:00", MySQLTimeUtils.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, ZonedDateTime.parse("2021-03-16 19:26:00.999999+04:00", MySQLTimeUtils.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp");

        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, "2021-03-16 19:26:00.999999", "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, LocalDateTime.parse("2021-03-16 19:26:00.999999", MySQLTimeUtils.MYSQL_DATETIME_FORMATTER), "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, OffsetDateTime.parse("2021-03-16 19:26:00.999999+03:00", MySQLTimeUtils.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, ZonedDateTime.parse("2021-03-16 19:26:00.999999+04:00", MySQLTimeUtils.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp1");

        LOG.info("datetimeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void stringBindAndExtract() {
        LOG.info("stringBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        String bindParma = "  \"\\'秦军\\'\\'\\'\\'\\'\\'\"\\Z\n\r update mysql_types as t set t.name = 'evil' where t.id = 5 %% " + '\032';

        assertStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, bindParma, "name");
        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, bindParma, "my_char");

        bindParma = "  'evil' , t.my_decimal = 999999.00   ";
        assertStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, bindParma, "name");
        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, bindParma, "my_char");

        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, "             ", "my_char");


        LOG.info("stringBindAndExtract test success");
        releaseConnection(taskAdjutant);

    }

    @Test(timeOut = TIME_OUT)
    public void binaryBindAndExtract() {
        LOG.info("binaryBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        String bindParam = "'china' %_#\\'\\' \" '秦军' '中国' \00   ";
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, bindParam, "my_var_binary");
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.BINARY, bindParam, "my_binary");

        bindParam = "  'evil' , t.my_decimal = 999999.00   ";
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, bindParam, "my_var_binary");
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.BINARY, bindParam, "my_binary");

        LOG.info("binaryBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void bitBindAndExtract() {
        LOG.info("bitBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, -1L, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Long.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Long.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, 0L, "my_bit");

        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Integer.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Integer.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, (short) -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Short.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Short.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, (short) 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, (byte) -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Byte.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, Byte.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, (byte) 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, "101001010010101", "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, MySQLNumberUtils.longToBigEndianBytes(-1L), "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLType.BIT, MySQLNumberUtils.longToBigEndianBytes(0L), "my_bit");

        LOG.info("bitBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyint1BindExtract() {
        LOG.info("tinyint1BindExtract test start");

        Map<String, String> map = new HashMap<>();
        map.put(PropertyKey.tinyInt1isBit.getKey(), "true");
        map.put(PropertyKey.transformedBitIsBoolean.getKey(), "false");

        MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(map);
        ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();
        assertNotNull(protocol, "protocol");

        MySQLTaskAdjutant taskAdjutant;
        taskAdjutant = protocol.taskExecutor.getAdjutant();

        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.TINYINT, 0);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BIT, 0);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, 0);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, false);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "true");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "T");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "Y");

        protocol.closeGracefully()
                .block();

        map.put(PropertyKey.transformedBitIsBoolean.getKey(), "true");
        sessionAdjutant = getSessionAdjutantForSingleHost(map);
        protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();
        assertNotNull(protocol, "protocol");
        taskAdjutant = protocol.taskExecutor.getAdjutant();

        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.TINYINT, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BIT, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, true);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "true");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "T");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "Y");

        protocol.closeGracefully()
                .block();

        map.put(PropertyKey.tinyInt1isBit.getKey(), "false");
        map.put(PropertyKey.transformedBitIsBoolean.getKey(), "false");
        sessionAdjutant = getSessionAdjutantForSingleHost(map);
        protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                .block();
        assertNotNull(protocol, "protocol");
        taskAdjutant = protocol.taskExecutor.getAdjutant();

        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.TINYINT, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BIT, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, 1);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, true);
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "true");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "T");
        assertTinyInt1BindAndExtract(taskAdjutant, MySQLType.BOOLEAN, "Y");

        protocol.closeGracefully()
                .block();

        LOG.info("tinyint1BindExtract test success");

    }

    @Test(timeOut = 10 * 1000L)
    public void numberBindAndExtract() {
        LOG.info("numberBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final String id = "7";
        String field = "my_tinyint";
        MySQLType mySQLType = MySQLType.TINYINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) -1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MAX_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MIN_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_tinyint_unsigned";
        mySQLType = MySQLType.TINYINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFF, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_smallint";
        mySQLType = MySQLType.SMALLINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) -1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MAX_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MIN_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_smallint_unsigned";
        mySQLType = MySQLType.SMALLINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_mediumint";
        mySQLType = MySQLType.MEDIUMINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -0x7FFF_FF, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0x7FFF_FF, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_mediumint_unsigned";
        mySQLType = MySQLType.MEDIUMINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FF, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_int";
        mySQLType = MySQLType.INT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MIN_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MAX_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_int_unsigned";
        mySQLType = MySQLType.INT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FFFFL, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_bigint";
        mySQLType = MySQLType.BIGINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MIN_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MAX_VALUE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_bigint_unsigned";
        mySQLType = MySQLType.BIGINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, MySQLNumberUtils.MAX_UNSIGNED_LONG, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_decimal";
        mySQLType = MySQLType.DECIMAL;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("-34234234.09"), field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);

        field = "my_decimal_unsigned";
        mySQLType = MySQLType.DECIMAL_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.1"), field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);


        field = "my_float";
        mySQLType = MySQLType.FLOAT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);

        field = "my_float_unsigned";
        mySQLType = MySQLType.FLOAT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0F, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field, id);

        field = "my_double";
        mySQLType = MySQLType.DOUBLE;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field, id);

        field = "my_double_unsigned";
        mySQLType = MySQLType.DOUBLE_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0D, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field, id);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field, id);


        LOG.info("numberBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void enumBindAndExtract() {
        LOG.info("enumBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertEnumBindExtract(taskAdjutant, MySQLType.CHAR, TrueOrFalse.T.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.CHAR, TrueOrFalse.F.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.T.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.F.name());

        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.T);
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.F);

        LOG.info("enumBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void setTypeBindAndExtract() {
        LOG.info("setTypeBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.BEIJING.name());
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.AOMENG.name());
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.SHANGHAI);
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, MySQLArrayUtils.asUnmodifiableSet(City.BEIJING.name(), City.SHANGHAI.name(), City.SHENZHEN.name(), City.TAIBEI.name()));

        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, EnumSet.of(City.BEIJING, City.SHANGHAI, City.SHENZHEN, City.TAIBEI));
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, City.AOMENG);
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, "AOMENG,SHANGHAI");
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, "AOMENG");

        LOG.info("setTypeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void jsonBindAndExtract() throws Exception {
        LOG.info("jsonBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();


        Map<String, Object> map = new HashMap<>();
        map.put("id", 1L);
        map.put("name", "''''\"\",\\,_%\032     ");

        final String id = "10", field = "my_json";
        final ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(map);
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.JSON, json, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final String resultJson = resultRow.get("field", String.class);

        final JsonNode root = mapper.readTree(resultJson);
        assertEquals(root.at("/id").asLong(), map.get("id"));
        assertEquals(root.at("/name").asText(), map.get("name"), field);

        LOG.info("jsonBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryBindAndExtract() {
        LOG.info("geometryBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = "POLYGON((0 0,0 1,0 3,0 0))";
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POINT with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_LINE_STRING with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POLYGON with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        final String point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection;

        point = String.format(" POINT  ( %s %s)  ", Double.MAX_VALUE, Double.MIN_VALUE);
        lineString = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        polygon = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPoint = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        multiLineString = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPolygon = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        geometryCollection = "GEOMETRYCOLLECTION(POINT(0 0))";


        wktText = String.format("GEOMETRYCOLLECTION (%s,%s,%s,%s ,%s,%s,%s) "
                , point
                , lineString
                , polygon
                , multiPoint

                , multiLineString
                , multiPolygon
                , geometryCollection
        );
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse GEOMETRY_COLLECTION with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("geometryBindAndExtract test success");
        releaseConnection(taskAdjutant);

    }

    @Test(timeOut = TIME_OUT)
    public void pointBindAndExtract() {
        LOG.info("pointBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.pointToWkb(wktText, true);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.pointToWkb(wktText, false);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        LOG.info("pointBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void lineStringBindAndExtract() {
        LOG.info("lineStringBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.lineStringToWkb(wktText, true);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.lineStringToWkb(wktText, false);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("lineStringBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void polygonBindAndExtract() {
        LOG.info("polygonBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);

        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = Geometries.polygonToWkb(wktText, true);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.polygonToWkb(wktText, false);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);
        LOG.info("polygonBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPointBindExtract() {
        LOG.info("multiPointBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("MULTIPOINT(( 0 0 ),(1 1),(1 3), (0 0),(%s %s)) "
                , Double.MIN_VALUE, Double.MAX_VALUE);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POINT with big endian .
//        wkbArray = Geometries.multiPointToWkb(wktText, true);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.multiPointToWkb(wktText, false);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiPointBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void multiLineStringBindExtract() {
        LOG.info("multiLineStringBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_LINE_STRING with big endian .
//        wkbArray = Geometries.multiLineStringToWkb(wktText, true);
//        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.multiLineStringToWkb(wktText, false);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiLineStringBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPolygonBindExtract() {
        LOG.info("multiPolygonBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POLYGON with big endian .
//        wkbArray = Geometries.multiPolygonToWkb(wktText, true);
//        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.multiPolygonToWkb(wktText, false);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiPolygonBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryCollectionBindExtract() {
        LOG.info("geometryCollectionBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;


        final String point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection;

        point = String.format(" POINT  ( %s %s)  ", Double.MAX_VALUE, Double.MIN_VALUE);
        lineString = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        polygon = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPoint = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        multiLineString = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPolygon = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        geometryCollection = "GEOMETRYCOLLECTION(POINT(0 0))";


        wktText = String.format("GEOMETRYCOLLECTION (%s,%s,%s,%s ,%s,%s,%s) "
                , point
                , lineString
                , polygon
                , multiPoint

                , multiLineString
                , multiPolygon
                , geometryCollection
        );
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse GEOMETRY_COLLECTION with big endian .
//        wkbArray = Geometries.geometryCollectionToWkb(wktText, true);
//        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryCollectionToWkb(wktText, false);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("geometryCollectionBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #geometryBindAndExtract()
     */
    private void assertGeometryBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "11", field = "my_geometry";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.geometryToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #pointBindAndExtract()
     */
    private void assertPointsBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {

        final String id = "12", field = "my_point";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.pointToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #lineStringBindAndExtract()
     */
    private void assertLineStringBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {

        final String id = "13", field = "my_linestring";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.lineStringToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #polygonBindAndExtract()
     */
    private void assertPolygonBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {

        final String id = "14", field = "my_polygon";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.polygonToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #multiPointBindExtract()
     */
    private void assertMultiPointBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "15", field = "my_multipoint";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.multiPointToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }


    /**
     * @see #multiLineStringBindExtract()
     */
    private void assertMultiLineStringBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "16", field = "my_multilinestring";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.multiLineStringToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #multiPolygonBindExtract()
     */
    private void assertMultiPolygonBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "17", field = "my_multipolygon";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.multiPolygonToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #geometryCollectionBindExtract()
     */
    private void assertGeometryCollectionBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "18", field = "my_geometrycollection";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = Geometries.geometryCollectionToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(Geometries.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #assertSetTypeBindAndExtract(MySQLTaskAdjutant, MySQLType, Object)
     */
    private void assertSetTypeBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {

        final String id = "9", field = "my_set";

        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final Set<City> citySet = resultRow.getSet("field", City.class);
        assertNotNull(citySet, field);

        final Set<City> bindSet;
        if (bindParam instanceof String) {
            Set<String> itemSet = MySQLStringUtils.spitAsSet((String) bindParam, ",");
            bindSet = MySQLStringUtils.convertStringsToEnumSet(itemSet, City.class);
        } else if (bindParam instanceof City) {
            bindSet = Collections.singleton((City) bindParam);
        } else if (bindParam instanceof Set) {
            Set<?> paramSet = (Set<?>) bindParam;
            Set<City> tempSet = new HashSet<>((int) (paramSet.size() / 0.75F));
            for (Object s : paramSet) {
                if (s instanceof String) {
                    tempSet.add(City.valueOf((String) s));
                } else if (s instanceof City) {
                    tempSet.add((City) s);
                }
            }
            bindSet = Collections.unmodifiableSet(tempSet);
        } else {
            throw new IllegalArgumentException("bindParam type error");
        }

        assertEquals(citySet, bindSet, field);

    }

    private void assertEnumBindExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final String id = "8", field = "my_enum";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final TrueOrFalse trueOrFalse = resultRow.get("field", TrueOrFalse.class);
        assertNotNull(trueOrFalse, field);
        if (bindParam instanceof String) {
            assertEquals(trueOrFalse, TrueOrFalse.valueOf((String) bindParam), field);
        } else {
            assertEquals(trueOrFalse, bindParam, field);
        }
    }

    /**
     * @see #numberBindAndExtract()
     */
    private void assertNumberBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field, final String id) {
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final MySQLType fieldType = (MySQLType) resultRow.getRowMeta().getSQLType("field");
        final Number resultValue = (Number) resultRow.get("field", fieldType.javaType());
        assertNotNull(resultValue, field);
        if (resultValue instanceof BigDecimal) {
            if (bindParam instanceof Float) {
                throw new IllegalArgumentException("bindParam type error");
            }
            final BigDecimal bindValue;
            if (bindParam instanceof String) {
                bindValue = new BigDecimal((String) bindParam);
            } else if (mySQLType == MySQLType.DOUBLE_UNSIGNED && bindParam instanceof Double) {
                bindValue = BigDecimal.valueOf((Double) bindParam);
            } else {
                bindValue = MySQLNumberUtils.convertNumberToBigDecimal((Number) bindParam);
            }
            assertEquals(((BigDecimal) resultValue).compareTo(bindValue), 0, field);
        } else if (resultValue instanceof BigInteger) {
            if (bindParam instanceof Double || bindParam instanceof Float) {
                throw new IllegalArgumentException("bindParam type error");
            }
            if (bindParam instanceof String) {
                assertEquals(resultValue, new BigInteger((String) bindParam), field);
            } else {
                assertEquals(resultValue, MySQLNumberUtils.convertNumberToBigInteger((Number) bindParam), field);
            }
        } else if (resultValue instanceof Double) {
            double bindValue;
            if (bindParam instanceof String) {
                bindValue = Double.parseDouble((String) bindParam);
            } else if (bindParam instanceof Double || bindParam instanceof Float) {
                bindValue = ((Number) bindParam).doubleValue();
            } else {
                throw new IllegalArgumentException("bindParam type error");
            }
            assertEquals(resultValue.doubleValue(), bindValue, field);
        } else if (resultValue instanceof Float) {
            float bindValue;
            if (bindParam instanceof String) {
                bindValue = Float.parseFloat((String) bindParam);
            } else if (bindParam instanceof Float) {
                bindValue = ((Number) bindParam).floatValue();
            } else {
                throw new IllegalArgumentException("bindParam type error");
            }
            assertEquals(resultValue.floatValue(), bindValue, field);
        } else {
            long bindValue;
            if (bindParam instanceof String) {
                bindValue = Long.parseLong((String) bindParam);
            } else {
                bindValue = MySQLNumberUtils.convertNumberToLong((Number) bindParam);
            }
            assertEquals(resultValue.longValue(), bindValue, field);
        }

    }

    /**
     * @see #tinyint1BindExtract()
     */
    private void assertTinyInt1BindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {

        final String field = "my_tinyint1";
        final String id = "6";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        if (bindParam instanceof Boolean) {
            assertEquals(resultRow.get("field", Boolean.class), bindParam, field);
        } else if (bindParam instanceof String) {
            Boolean bindValue = MySQLConvertUtils.tryConvertToBoolean((String) bindParam);
            assertEquals(resultRow.get("field", Boolean.class), bindValue, field);
        } else {
            Long resultValue = resultRow.get("field", Long.class);
            assertNotNull(resultValue, field);
            assertEquals(resultValue.longValue(), ((Number) bindParam).longValue(), field);
        }

    }

    /**
     * @see #bitBindAndExtract()
     */
    private void assertBitBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field) {
        final String id = "5";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final Long bits = resultRow.get("field", Long.class);
        assertNotNull(bits, field);

        if (bindParam instanceof Long) {
            assertEquals(bits, bindParam, field);
        } else if (bindParam instanceof Integer) {
            long bindBits = (Integer) bindParam & 0xFFFF_FFFFL;
            assertEquals(bits.longValue(), bindBits, field);
        } else if (bindParam instanceof Short) {
            long bindBits = (Short) bindParam & 0xFFFFL;
            assertEquals(bits.longValue(), bindBits, field);
        } else if (bindParam instanceof Byte) {
            long bindBits = (Byte) bindParam & 0xFFL;
            assertEquals(bits.longValue(), bindBits, field);
        } else if (bindParam instanceof String) {
            long bindBits = Long.parseLong((String) bindParam, 2);
            assertEquals(bits.longValue(), bindBits, field);
        } else if (bindParam instanceof byte[]) {
            byte[] bytes = (byte[]) bindParam;
            long bindBits = MySQLNumberUtils.readLongFromBigEndian(bytes, 0, bytes.length);
            assertEquals(bits.longValue(), bindBits, field);
        } else {
            // never here
            throw new IllegalArgumentException("bindParam error");
        }


    }

    /**
     * @see #stringBindAndExtract()
     */
    private void assertBinaryBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final String id = "4";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        // 3. compare
        final byte[] bytes = resultRow.get("field", byte[].class);
        assertNotNull(bytes, field);
        final byte[] bindByteArray = bindParam.getBytes(taskAdjutant.obtainCharsetClient());
        if (mySQLType == MySQLType.BINARY) {
            final int length = Math.min(bindByteArray.length, bytes.length);
            for (int i = 0; i < length; i++) {
                if (bytes[i] != bindByteArray[i]) {
                    fail("binary type assert failure");
                }
            }
        } else {
            assertTrue(Arrays.equals(bytes, bindByteArray), field);
        }


    }


    /**
     * @see #stringBindAndExtract()
     */
    private void assertStringBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final String id = "3";

        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String string = resultRow.get("field", String.class);
        assertNotNull(string, field);
        if (mySQLType == MySQLType.CHAR) {
            final String actualBindParam = MySQLStringUtils.trimTrailingSpace(bindParam);
            if (taskAdjutant.obtainServer().containSqlMode(SQLMode.PAD_CHAR_TO_FULL_LENGTH)) {
                assertTrue(string.startsWith(actualBindParam), field);
                final String tailingSpaces = string.substring(actualBindParam.length());
                assertFalse(MySQLStringUtils.hasText(tailingSpaces), "tailingSpaces");
            } else {
                assertEquals(string, actualBindParam, field);
            }
        } else {
            assertEquals(string, bindParam, field);
        }


    }

    /**
     * @see #datetimeBindAndExtract()
     */
    private void assertDateTimeModify(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindDatetime, final String field) {
        final String id = "2";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindDatetime, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);


        final LocalDateTime expectDateTime;

        if (bindDatetime instanceof LocalDateTime) {
            expectDateTime = (LocalDateTime) bindDatetime;
        } else if (bindDatetime instanceof String) {
            expectDateTime = LocalDateTime.parse((String) bindDatetime, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (bindDatetime instanceof OffsetDateTime) {
            expectDateTime = ((OffsetDateTime) bindDatetime)
                    .withOffsetSameInstant(taskAdjutant.obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else if (bindDatetime instanceof ZonedDateTime) {
            expectDateTime = ((ZonedDateTime) bindDatetime)
                    .withZoneSameInstant(taskAdjutant.obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else {
            // never here
            throw new IllegalArgumentException("bindDatetime type error");
        }


        final LocalDateTime dateTime = resultRow.get("field", LocalDateTime.class);
        assertNotNull(dateTime, field);
        final String dateTimeText = dateTime.format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);

        Properties<PropertyKey> properties = taskAdjutant.obtainHostInfo().getProperties();
        if (properties.getOrDefault(PropertyKey.timeTruncateFractional, Boolean.class)) {
            String bindDateTimeText = expectDateTime.format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
            if (!bindDateTimeText.startsWith(dateTimeText)) {
                fail(String.format("dateTimeText[%s] and bindDateTimeText[%s] not match.", dateTimeText, bindDateTimeText));
            }
        } else {
            Duration duration = Duration.between(expectDateTime, dateTime);
            if (duration.isNegative() || duration.getSeconds() > 1L) {
                fail(String.format("create time[%s] and expectDateTime[%s] not match.", dateTime, expectDateTime));
            }
        }


    }

    private void updateSingleField(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field, final String id) {

        final String paramExp;
        switch (field) {
            case "my_geometry":
            case "my_point":
            case "my_polygon":
            case "my_linestring":
            case "my_multipoint":
            case "my_multilinestring":
            case "my_multipolygon":
            case "my_geometrycollection":
                if (bindParam instanceof String) {
                    paramExp = "ST_GEOMETRYFROMTEXT(?)";
                } else {
                    paramExp = "ST_GEOMETRYFROMWKB(?)";
                }
                break;
            default:
                paramExp = "?";
        }
        String sql = String.format("UPDATE mysql_types as t SET t.%s = %s WHERE t.id = ?", field, paramExp);

        List<BindValue> bindValueList = new ArrayList<>(2);

        BindValue bindValue = MySQLBindValue.create(0, mySQLType, bindParam);

        bindValueList.add(bindValue);
        bindValue = MySQLBindValue.create(1, MySQLType.BIGINT, id);
        bindValueList.add(bindValue);

        ResultStates resultStates;
        resultStates = ComQueryTask.bindableUpdate(StmtWrappers.multi(sql, bindValueList), taskAdjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "getAffectedRows");
    }

    private ResultRow querySingleField(final MySQLTaskAdjutant taskAdjutant, final String field, final String id) {
        String sql = String.format("SELECT t.id as id, t.%s as field FROM mysql_types as t WHERE t.id = ?", field);
        BindValue bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, id);

        List<ResultRow> resultRowList;
        resultRowList = ComQueryTask.bindableQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();
        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 1L, "resultRowList size");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        return resultRow;
    }


    /**
     * @see #mysqlTypeMetadataMatch()
     */
    private String createQuerySqlForMySQLTypeMatch() {
        StringBuilder builder = new StringBuilder("SELECT")
                .append(" t.id as id")
                .append(",t.create_time as createTime")
                .append(",t.update_time as updateTime")
                .append(",t.name as name")

                .append(",t.my_char as myChar")
                .append(",t.my_binary as myBinary")
                .append(",t.my_var_binary as myVarBinary")
                .append(",t.my_bit as myBit")

                .append(",t.my_tinyint1 as myTinyint1")
                .append(",t.my_tinyint as myTinyint")
                .append(",t.my_tinyint_unsigned as myTinyintUnsigned")
                .append(",t.my_boolean as myBoolean")

                .append(",t.my_smallint as mySmallint")
                .append(",t.my_smallint_unsigned as mySmallintUnsigned")
                .append(",t.my_mediumint as myMediumint")
                .append(",t.my_mediumint_unsigned as myMediumintUnsigned")

                .append(",t.my_int as myInt")
                .append(",t.my_int_unsigned as myIntUnsigned")
                .append(",t.my_bigint_unsigned as myBigintUnsigned")
                .append(",t.my_decimal as myDecimal")

                .append(",t.my_decimal_unsigned as myDecimalUnsigned")
                .append(",t.my_float as myFloat")
                .append(",t.my_float_unsigned as myFloatUnsigned")
                .append(",t.my_double as myDouble")

                .append(",t.my_double_unsigned as myDoubleUnsigned")
                .append(",t.my_tiny_text as myTinyText")
                .append(",t.my_enum as myEnum")
                .append(",t.my_set as mySet")

                .append(",t.my_json as myJson")
                .append(",t.my_geometry as myGeometry")
                .append(",t.my_point as myPoint")
                .append(",t.my_linestring as myLinestring")

                .append(",t.my_polygon as myPolygon")
                .append(",t.my_multipoint as myMultipoint")
                .append(",t.my_multilinestring as myMultilinestring")
                .append(",t.my_multipolygon as myMultipolygon")

                .append(",t.my_geometrycollection as myGeometrycollection")
                .append(",t.my_timestamp as myTimestamp")
                .append(",t.my_timestamp1 as myTimestamp1");

        return builder
                .append(" FROM mysql_types as t ORDER BY t.id DESC LIMIT 10")
                .toString();
    }


    private void assertQueryResultRowMySQLTypeMatch(final ResultRow row, final Properties<PropertyKey> properties) {
        final ResultRowMeta rowMeta = row.getRowMeta();

        final Object id = row.get("id");
        assertNotNull(id, "id");
        assertTrue(id instanceof Long, "id java class");
        assertEquals(rowMeta.getSQLType("id"), MySQLType.BIGINT, "id mysql type");
        assertFalse(rowMeta.isUnsigned("id"), "id isUnsigned");
        assertEquals(rowMeta.getNullMode("id"), NullMode.NON_NULL, "id null mode.");
        assertTrue(rowMeta.isAutoIncrement("id"), "id isAutoIncrement");


        final LocalDateTime createTime = row.get("createTime", LocalDateTime.class);
        assertNotNull(createTime, "createTime");
        assertEquals(rowMeta.getSQLType("createTime"), MySQLType.DATETIME, "createTime mysql type");
        assertEquals(rowMeta.getPrecision("createTime"), 0, "createTime precision");
        assertEquals(rowMeta.getNullMode("createTime"), NullMode.NON_NULL, "createTime null mode.");

        final LocalDateTime updateTime = row.get("updateTime", LocalDateTime.class);
        assertNotNull(updateTime, "updateTime");
        assertEquals(rowMeta.getSQLType("updateTime"), MySQLType.DATETIME, "updateTime mysql type");
        assertEquals(rowMeta.getPrecision("updateTime"), 6, "updateTime precision");
        assertEquals(rowMeta.getNullMode("updateTime"), NullMode.NON_NULL, "updateTime null mode.");

        final String name = row.get("name", String.class);
        assertNotNull(name, "name");
        assertEquals(rowMeta.getSQLType("name"), MySQLType.VARCHAR, "name mysql type");
        assertEquals(rowMeta.getNullMode("name"), NullMode.NON_NULL, "name null mode.");

        final String myChar = row.get("myChar", String.class);
        assertNotNull(myChar, "myChar");
        assertEquals(rowMeta.getSQLType("myChar"), MySQLType.CHAR, "myChar mysql type");
        assertEquals(rowMeta.getNullMode("myChar"), NullMode.NON_NULL, "myChar null mode.");

        final byte[] myBinary = row.get("myBinary", byte[].class);
        assertNotNull(myBinary, "myBinary");
        assertEquals(rowMeta.getSQLType("myBinary"), MySQLType.BINARY, "myBinary mysql type");
        assertEquals(rowMeta.getNullMode("myBinary"), NullMode.NON_NULL, "myBinary null mode.");

        final byte[] myVarBinary = row.get("myVarBinary", byte[].class);
        assertNotNull(myVarBinary, "myVarBinary");
        assertEquals(rowMeta.getSQLType("myVarBinary"), MySQLType.VARBINARY, "myVarBinary mysql type");
        assertEquals(rowMeta.getNullMode("myVarBinary"), NullMode.NON_NULL, "myVarBinary null mode.");

        final Long myBit = row.get("myBit", Long.class);
        assertNotNull(myBit, "myBit");
        assertEquals(rowMeta.getSQLType("myBit"), MySQLType.BIT, "myBit mysql type");
        assertEquals(Long.toBinaryString(myBit), row.get("myBit", String.class), "myBit string");
        assertEquals(rowMeta.getNullMode("myBit"), NullMode.NON_NULL, "myBit null mode.");

        final Byte myTinyint1 = row.get("myTinyint1", Byte.class);
        assertNotNull(myTinyint1, "myTinyint1");
        assertTinyInt1Type(row, "myTinyint1", properties);
        assertFalse(rowMeta.isUnsigned("myTinyint1"), "myTinyint1 isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyint1"), NullMode.NON_NULL, "myTinyint1 null mode.");

        final Byte myTinyint = row.get("myTinyint", Byte.class);
        assertNotNull(myTinyint, "myTinyint");
        assertEquals(rowMeta.getSQLType("myTinyint"), MySQLType.TINYINT, "myTinyint mysql type");
        assertFalse(rowMeta.isUnsigned("myTinyint"), "myTinyint isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyint"), NullMode.NON_NULL, "myTinyint null mode.");

        // below tiny_unsigned assert
        final Integer myTinyintUnsigned = row.get("myTinyintUnsigned", Integer.class);
        assertNotNull(myTinyintUnsigned, "myTinyintUnsigned");
        assertEquals(rowMeta.getSQLType("myTinyintUnsigned"), MySQLType.TINYINT_UNSIGNED, "myTinyintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myTinyintUnsigned"), "myTinyintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyintUnsigned"), NullMode.NON_NULL, "myTinyintUnsigned null mode.");

        // below boolean assert
        final Object myBoolean = row.get("myBoolean");
        assertNotNull(myBoolean, "myBoolean");
        assertTinyInt1Type(row, "myBoolean", properties);
        assertEquals(rowMeta.getNullMode("myBoolean"), NullMode.NON_NULL, "myBoolean null mode.");

        // below smallint assert
        final Object mySmallint = row.get("mySmallint");
        assertNotNull(mySmallint, "mySmallint");
        assertTrue(mySmallint instanceof Short, "mySmallint java class.");
        assertEquals(rowMeta.getSQLType("mySmallint"), MySQLType.SMALLINT, "mySmallint mysql type");
        assertFalse(rowMeta.isUnsigned("mySmallint"), "mySmallint isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallint"), NullMode.NON_NULL, "mySmallint null mode.");

        // below smallint_unsigned assert
        final Object mySmallintUnsigned = row.get("mySmallintUnsigned");
        assertNotNull(mySmallintUnsigned, "mySmallintUnsigned");
        assertTrue(mySmallintUnsigned instanceof Integer, "mySmallintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("mySmallintUnsigned"), MySQLType.SMALLINT_UNSIGNED, "mySmallintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("mySmallintUnsigned"), "mySmallintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallintUnsigned"), NullMode.NON_NULL, "mySmallintUnsigned null mode.");

        // below mediumint assert
        final Object myMediumint = row.get("myMediumint");
        assertNotNull(myMediumint, "myMediumint");
        assertTrue(myMediumint instanceof Integer, "myMediumint java class.");
        assertEquals(rowMeta.getSQLType("myMediumint"), MySQLType.MEDIUMINT, "myMediumint mysql type");
        assertFalse(rowMeta.isUnsigned("myMediumint"), "myMediumint isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumint"), NullMode.NON_NULL, "myMediumint null mode.");

        // below mediumint_unsigned  assert
        final Object myMediumintUnsigned = row.get("myMediumintUnsigned");
        assertNotNull(myMediumintUnsigned, "myMediumintUnsigned");
        assertTrue(myMediumintUnsigned instanceof Integer, "myMediumintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myMediumintUnsigned"), MySQLType.MEDIUMINT_UNSIGNED, "myMediumintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myMediumintUnsigned"), "myMediumintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumintUnsigned"), NullMode.NON_NULL, "myMediumintUnsigned null mode.");

        // blow int assert
        final Object myInt = row.get("myInt");
        assertNotNull(myInt, "myInt");
        assertTrue(myInt instanceof Integer, "myInt java class.");
        assertEquals(rowMeta.getSQLType("myInt"), MySQLType.INT, "myInt mysql type");
        assertFalse(rowMeta.isUnsigned("myInt"), "myInt isUnsigned");
        assertEquals(rowMeta.getNullMode("myInt"), NullMode.NON_NULL, "myInt null mode.");

        // below int_unsigned assert
        final Object myIntUnsigned = row.get("myIntUnsigned");
        assertNotNull(myIntUnsigned, "myIntUnsigned");
        assertTrue(myIntUnsigned instanceof Long, "myIntUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myIntUnsigned"), MySQLType.INT_UNSIGNED, "myIntUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myIntUnsigned"), "myIntUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myIntUnsigned"), NullMode.NON_NULL, "myIntUnsigned null mode.");

        // below bigint_unsigned assert
        final Object myBigintUnsigned = row.get("myBigintUnsigned");
        assertNotNull(myBigintUnsigned, "myBigintUnsigned");
        assertTrue(myBigintUnsigned instanceof BigInteger, "myBigintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myBigintUnsigned"), MySQLType.BIGINT_UNSIGNED, "myBigintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myBigintUnsigned"), "myBigintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myBigintUnsigned"), NullMode.NON_NULL, "myBigintUnsigned null mode.");

        // below decimal assert
        final Object myDecimal = row.get("myDecimal");
        assertNotNull(myDecimal, "myDecimal");
        assertTrue(myDecimal instanceof BigDecimal, "myDecimal java class.");
        assertEquals(rowMeta.getSQLType("myDecimal"), MySQLType.DECIMAL, "myDecimal mysql type");
        assertFalse(rowMeta.isUnsigned("myDecimal"), "myDecimal isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimal"), 14, "myDecimal precision");
        assertEquals(rowMeta.getScale("myDecimal"), 2, "myDecimal scale");
        assertEquals(rowMeta.getNullMode("myDecimal"), NullMode.NON_NULL, "myDecimal null mode.");

        // below decimal_unsigned
        final Object myDecimalUnsigned = row.get("myDecimalUnsigned");
        assertNotNull(myDecimalUnsigned, "myDecimalUnsigned");
        assertTrue(myDecimalUnsigned instanceof BigDecimal, "myDecimalUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myDecimalUnsigned"), MySQLType.DECIMAL_UNSIGNED, "myDecimalUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myDecimalUnsigned"), "myDecimalUnsigned isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimalUnsigned"), 14, "myDecimalUnsigned precision");
        assertEquals(rowMeta.getScale("myDecimalUnsigned"), 2, "myDecimalUnsigned scale");
        assertEquals(rowMeta.getNullMode("myDecimalUnsigned"), NullMode.NON_NULL, "myDecimalUnsigned null mode.");

        // below float assert
        final Object myFloat = row.get("myFloat");
        assertNotNull(myFloat, "myFloat");
        assertTrue(myFloat instanceof Float, "myFloat java class.");
        assertEquals(rowMeta.getSQLType("myFloat"), MySQLType.FLOAT, "myFloat mysql type");
        assertFalse(rowMeta.isUnsigned("myFloat"), "myFloat isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloat"), NullMode.NON_NULL, "myFloat null mode.");

        // below float_unsigned
        final Object myFloatUnsigned = row.get("myFloatUnsigned");
        assertNotNull(myFloatUnsigned, "myFloatUnsigned");
        assertTrue(myFloatUnsigned instanceof Double, "myFloatUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myFloatUnsigned"), MySQLType.FLOAT_UNSIGNED, "myFloatUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myFloatUnsigned"), "myFloatUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloatUnsigned"), NullMode.NON_NULL, "myFloatUnsigned null mode.");

        // below double assert
        final Object myDouble = row.get("myDouble");
        assertNotNull(myDouble, "myDouble");
        assertTrue(myDouble instanceof Double, "myDouble java class.");
        assertEquals(rowMeta.getSQLType("myDouble"), MySQLType.DOUBLE, "myDouble mysql type");
        assertFalse(rowMeta.isUnsigned("myDouble"), "myDouble isUnsigned");
        assertEquals(rowMeta.getNullMode("myDouble"), NullMode.NON_NULL, "myDouble null mode.");

        // below double_unsigned assert
        final Object myDoubleUnsigned = row.get("myDoubleUnsigned");
        assertNotNull(myDoubleUnsigned, "myDoubleUnsigned");
        assertTrue(myDoubleUnsigned instanceof BigDecimal, "myDoubleUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myDoubleUnsigned"), MySQLType.DOUBLE_UNSIGNED, "myDoubleUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myDoubleUnsigned"), "myDoubleUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myDoubleUnsigned"), NullMode.NON_NULL, "myDoubleUnsigned null mode.");

        // below enum assert
        final Object myEnum = row.get("myEnum");
        assertNotNull(myEnum, "myEnum");
        assertTrue(myEnum instanceof String, "myEnum java class.");
        final TrueOrFalse trueOrFalse = row.get("myEnum", TrueOrFalse.class);
        assertNotNull(trueOrFalse, "myEnum");
        assertEquals(rowMeta.getSQLType("myEnum"), MySQLType.ENUM, "myEnum mysql type");
        assertFalse(rowMeta.isUnsigned("myEnum"), "myEnum isUnsigned");
        assertEquals(rowMeta.getNullMode("myEnum"), NullMode.NON_NULL, "myEnum null mode.");

        // below set type assert

        final Object mySet = row.get("mySet");
        assertNotNull(myEnum, "mySet");
        assertTrue(mySet instanceof Set, "mySet java class.");
        assertEquals(rowMeta.getSQLType("mySet"), MySQLType.SET, "mySet mysql type");
        assertFalse(rowMeta.isUnsigned("mySet"), "mySet isUnsigned");
        assertEquals(rowMeta.getNullMode("mySet"), NullMode.NON_NULL, "mySet null mode.");

        @SuppressWarnings("unchecked") final Set<String> stringSet = (Set<String>) mySet;
        final Set<City> citySet = row.getSet("mySet", City.class);
        final List<City> cityList = row.getList("mySet", City.class);
        assertNotNull(citySet, "citySet");
        assertNotNull(cityList, "cityList");
        assertEquals(citySet.size(), stringSet.size(), "citySet size");
        assertEquals(cityList.size(), stringSet.size(), "cityList size");

        for (City city : citySet) {
            if (!stringSet.contains(city.name())) {
                fail(String.format("city[%s] not exits.", city));
            }
        }

        for (City city : cityList) {
            if (!stringSet.contains(city.name())) {
                fail(String.format("city[%s] not exits.", city));
            }
        }


        // below json type assert
        final Object myJson = row.get("myJson");
        assertNotNull(myJson, "myJson");
        assertTrue(myJson instanceof String, "myJson java class.");
        assertNotNull(trueOrFalse, "myJson");
        assertEquals(rowMeta.getSQLType("myJson"), MySQLType.JSON, "myJson mysql type");
        assertFalse(rowMeta.isUnsigned("myJson"), "myJson isUnsigned");
        assertEquals(rowMeta.getNullMode("myJson"), NullMode.NULLABLE, "myJson null mode.");

        // below geometry type assert
        assertEquals(rowMeta.getSQLType("myGeometry"), MySQLType.GEOMETRY, "myGeometry mysql type");
        assertFalse(rowMeta.isUnsigned("myGeometry"), "myGeometry isUnsigned");
        assertEquals(rowMeta.getNullMode("myGeometry"), NullMode.NULLABLE, "myGeometry null mode.");

        // below point type assert
        assertEquals(rowMeta.getSQLType("myPoint"), MySQLType.GEOMETRY, "myPoint mysql type");
        assertFalse(rowMeta.isUnsigned("myPoint"), "myPoint isUnsigned");
        assertEquals(rowMeta.getNullMode("myPoint"), NullMode.NULLABLE, "myPoint null mode.");


        // below linestring type assert
        assertEquals(rowMeta.getSQLType("myLinestring"), MySQLType.GEOMETRY, "myLinestring mysql type");
        assertFalse(rowMeta.isUnsigned("myLinestring"), "myLinestring isUnsigned");
        assertEquals(rowMeta.getNullMode("myLinestring"), NullMode.NULLABLE, "myLinestring null mode.");

        // below polygon type assert
        assertEquals(rowMeta.getSQLType("myPolygon"), MySQLType.GEOMETRY, "myPolygon mysql type");
        assertFalse(rowMeta.isUnsigned("myPolygon"), "myPolygon isUnsigned");
        assertEquals(rowMeta.getNullMode("myPolygon"), NullMode.NULLABLE, "myPolygon null mode.");

        // below multipoint type assert
        assertEquals(rowMeta.getSQLType("myMultipoint"), MySQLType.GEOMETRY, "myMultipoint mysql type");
        assertFalse(rowMeta.isUnsigned("myMultipoint"), "myMultipoint isUnsigned");
        assertEquals(rowMeta.getNullMode("myMultipoint"), NullMode.NULLABLE, "myMultipoint null mode.");

        // below multilinestring type assert
        assertEquals(rowMeta.getSQLType("myMultilinestring"), MySQLType.GEOMETRY, "myMultilinestring mysql type");
        assertFalse(rowMeta.isUnsigned("myMultilinestring"), "myMultilinestring isUnsigned");
        assertEquals(rowMeta.getNullMode("myMultilinestring"), NullMode.NULLABLE, "myMultilinestring null mode.");

        // below multipolygon type assert
        assertEquals(rowMeta.getSQLType("myMultipolygon"), MySQLType.GEOMETRY, "myMultipolygon mysql type");
        assertFalse(rowMeta.isUnsigned("myMultipolygon"), "myMultipolygon isUnsigned");
        assertEquals(rowMeta.getNullMode("myMultipolygon"), NullMode.NULLABLE, "myMultipolygon null mode.");

        // below geometrycollection type assert
        assertEquals(rowMeta.getSQLType("myGeometrycollection"), MySQLType.GEOMETRY, "myGeometrycollection mysql type");
        assertFalse(rowMeta.isUnsigned("myGeometrycollection"), "myGeometrycollection isUnsigned");
        assertEquals(rowMeta.getNullMode("myGeometrycollection"), NullMode.NULLABLE, "myGeometrycollection null mode.");

        final LocalDateTime myTimestamp = row.get("myTimestamp", LocalDateTime.class);
        assertNotNull(myTimestamp, "myTimestamp");
        assertEquals(rowMeta.getSQLType("myTimestamp"), MySQLType.TIMESTAMP, "myTimestamp mysql type");
        assertEquals(rowMeta.getPrecision("myTimestamp"), 0, "myTimestamp precision");
        assertEquals(rowMeta.getNullMode("myTimestamp"), NullMode.NON_NULL, "myTimestamp null mode.");

        final Object myTimestamp1 = row.get("myTimestamp1");
        assertNotNull(myTimestamp1, "myTimestamp1");
        assertEquals(row.get("myTimestamp1"), myTimestamp1, "myTimestamp1 convert");
        assertEquals(rowMeta.getSQLType("myTimestamp1"), MySQLType.TIMESTAMP, "myTimestamp1 mysql type");
        assertEquals(rowMeta.getPrecision("myTimestamp1"), 1, "myTimestamp1 precision");
        assertEquals(rowMeta.getNullMode("myTimestamp1"), NullMode.NON_NULL, "myTimestamp1 null mode.");

    }

    private void assertTinyInt1Type(ResultRow row, String columnAlias, Properties<PropertyKey> properties) {
        final ResultRowMeta rowMeta = row.getRowMeta();

        assertFalse(rowMeta.isUnsigned(columnAlias), columnAlias + " isSigned");
        final SQLType myBooleanType = rowMeta.getSQLType(columnAlias);
        assertNotNull(row.get(columnAlias, Boolean.class), columnAlias + " convert to boolean");

        if (properties.getOrDefault(PropertyKey.tinyInt1isBit, Boolean.class)) {
            if (properties.getOrDefault(PropertyKey.transformedBitIsBoolean, Boolean.class)) {
                assertEquals(myBooleanType, MySQLType.BOOLEAN, columnAlias + " mysql type");
                assertTrue(row.get(columnAlias) instanceof Boolean, columnAlias + " is boolean type.");
            } else {
                assertEquals(myBooleanType, MySQLType.BIT, columnAlias + " mysql type");
                assertTrue(row.get(columnAlias) instanceof Long, columnAlias + " is Byte type.");
            }

        } else {
            assertEquals(myBooleanType, MySQLType.TINYINT, columnAlias + " mysql type");
            assertTrue(row.get(columnAlias) instanceof Byte, columnAlias + " is Byte type.");
        }

    }

    private void printMultiError(Throwable e) {
        if (e instanceof JdbdCompositeException) {
            int index = 0;
            for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                LOG.error("multi error:{} :\n", index, throwable);
                index++;
            }
        }


    }


    private MySQLTaskAdjutant obtainTaskAdjutant() {
        MySQLTaskAdjutant taskAdjutant;

        taskAdjutant = TASK_ADJUTANT_QUEUE.poll();
        if (taskAdjutant == null) {
            Map<String, String> map = new HashMap<>();
            if (ClientTestUtils.existsServerPublicKey()) {
                map.put(PropertyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
            }
            ClientTestUtils.appendZoneConfig(map);

            MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(map);
            ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, sessionAdjutant)
                    .block();
            assertNotNull(protocol, "protocol");

            taskAdjutant = protocol.taskExecutor.getAdjutant();
        }

        return taskAdjutant;
    }

    private void releaseConnection(MySQLTaskAdjutant adjutant) {
        TASK_ADJUTANT_QUEUE.add(adjutant);
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
