package io.jdbd.mysql.protocol.client;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.*;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.conf.Properties;
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
import java.time.LocalDateTime;
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
    }

    @Test
    public void mysqlTypeMatch() {
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
        releaseConnection(adjutant);
        LOG.info("mysqlTypeMatch test success");
    }



    /*################################## blow private method ##################################*/

    /**
     * @see #mysqlTypeMatch()
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

                .append(",t.my_geometrycollection as myGeometrycollection");

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
        assertEquals(rowMeta.getScale("createTime"), 0, "createTime precision");
        assertEquals(rowMeta.getNullMode("createTime"), NullMode.NON_NULL, "createTime null mode.");

        final LocalDateTime updateTime = row.get("updateTime", LocalDateTime.class);
        assertNotNull(updateTime, "updateTime");
        assertEquals(rowMeta.getSQLType("updateTime"), MySQLType.DATETIME, "updateTime mysql type");
        assertEquals(rowMeta.getScale("updateTime"), 6, "updateTime precision");
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

        final Integer myTinyintUnsigned = row.get("myTinyintUnsigned", Integer.class);
        assertNotNull(myTinyintUnsigned, "myTinyintUnsigned");
        assertEquals(rowMeta.getSQLType("myTinyintUnsigned"), MySQLType.TINYINT_UNSIGNED, "myTinyintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myTinyintUnsigned"), "myTinyintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyintUnsigned"), NullMode.NON_NULL, "myTinyintUnsigned null mode.");

        final Object myBoolean = row.get("myBoolean");
        assertNotNull(myBoolean, "myBoolean");
        assertTinyInt1Type(row, "myBoolean", properties);
        assertEquals(rowMeta.getNullMode("myBoolean"), NullMode.NON_NULL, "myBoolean null mode.");

        final Object mySmallint = row.get("mySmallint");
        assertNotNull(mySmallint, "mySmallint");
        assertTrue(mySmallint instanceof Short, "mySmallint java class.");
        assertEquals(rowMeta.getSQLType("mySmallint"), MySQLType.SMALLINT, "mySmallint mysql type");
        assertFalse(rowMeta.isUnsigned("mySmallint"), "mySmallint isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallint"), NullMode.NON_NULL, "mySmallint null mode.");

        final Object mySmallintUnsigned = row.get("mySmallintUnsigned");
        assertNotNull(mySmallintUnsigned, "mySmallintUnsigned");
        assertTrue(mySmallintUnsigned instanceof Integer, "mySmallintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("mySmallintUnsigned"), MySQLType.SMALLINT_UNSIGNED, "mySmallintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("mySmallintUnsigned"), "mySmallintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallintUnsigned"), NullMode.NON_NULL, "mySmallintUnsigned null mode.");

        final Object myMediumint = row.get("myMediumint");
        assertNotNull(myMediumint, "myMediumint");
        assertTrue(myMediumint instanceof Integer, "myMediumint java class.");
        assertEquals(rowMeta.getSQLType("myMediumint"), MySQLType.MEDIUMINT, "myMediumint mysql type");
        assertFalse(rowMeta.isUnsigned("myMediumint"), "myMediumint isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumint"), NullMode.NON_NULL, "myMediumint null mode.");

        final Object myMediumintUnsigned = row.get("myMediumintUnsigned");
        assertNotNull(myMediumintUnsigned, "myMediumintUnsigned");
        assertTrue(myMediumintUnsigned instanceof Integer, "myMediumintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myMediumintUnsigned"), MySQLType.MEDIUMINT_UNSIGNED, "myMediumintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myMediumintUnsigned"), "myMediumintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumintUnsigned"), NullMode.NON_NULL, "myMediumintUnsigned null mode.");

        final Object myInt = row.get("myInt");
        assertNotNull(myInt, "myInt");
        assertTrue(myInt instanceof Integer, "myInt java class.");
        assertEquals(rowMeta.getSQLType("myInt"), MySQLType.INT, "myInt mysql type");
        assertFalse(rowMeta.isUnsigned("myInt"), "myInt isUnsigned");
        assertEquals(rowMeta.getNullMode("myInt"), NullMode.NON_NULL, "myInt null mode.");

        final Object myIntUnsigned = row.get("myIntUnsigned");
        assertNotNull(myIntUnsigned, "myIntUnsigned");
        assertTrue(myIntUnsigned instanceof Long, "myIntUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myIntUnsigned"), MySQLType.INT_UNSIGNED, "myIntUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myIntUnsigned"), "myIntUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myIntUnsigned"), NullMode.NON_NULL, "myIntUnsigned null mode.");


        final Object myBigintUnsigned = row.get("myBigintUnsigned");
        assertNotNull(myBigintUnsigned, "myBigintUnsigned");
        assertTrue(myBigintUnsigned instanceof BigInteger, "myBigintUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myBigintUnsigned"), MySQLType.BIGINT_UNSIGNED, "myBigintUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myBigintUnsigned"), "myBigintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myBigintUnsigned"), NullMode.NON_NULL, "myBigintUnsigned null mode.");


        final Object myDecimal = row.get("myDecimal");
        assertNotNull(myDecimal, "myDecimal");
        assertTrue(myDecimal instanceof BigDecimal, "myDecimal java class.");
        assertEquals(rowMeta.getSQLType("myDecimal"), MySQLType.DECIMAL, "myDecimal mysql type");
        assertFalse(rowMeta.isUnsigned("myDecimal"), "myDecimal isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimal"), 14, "myDecimal precision");
        assertEquals(rowMeta.getScale("myDecimal"), 2, "myDecimal scale");
        assertEquals(rowMeta.getNullMode("myDecimal"), NullMode.NON_NULL, "myDecimal null mode.");


        final Object myDecimalUnsigned = row.get("myDecimalUnsigned");
        assertNotNull(myDecimalUnsigned, "myDecimalUnsigned");
        assertTrue(myDecimalUnsigned instanceof BigDecimal, "myDecimalUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myDecimalUnsigned"), MySQLType.DECIMAL_UNSIGNED, "myDecimalUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myDecimalUnsigned"), "myDecimalUnsigned isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimalUnsigned"), 14, "myDecimalUnsigned precision");
        assertEquals(rowMeta.getScale("myDecimalUnsigned"), 2, "myDecimalUnsigned scale");
        assertEquals(rowMeta.getNullMode("myDecimalUnsigned"), NullMode.NON_NULL, "myDecimalUnsigned null mode.");


        final Object myFloat = row.get("myFloat");
        assertNotNull(myFloat, "myFloat");
        assertTrue(myFloat instanceof Float, "myFloat java class.");
        assertEquals(rowMeta.getSQLType("myFloat"), MySQLType.FLOAT, "myFloat mysql type");
        assertFalse(rowMeta.isUnsigned("myFloat"), "myFloat isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloat"), NullMode.NON_NULL, "myFloat null mode.");

        final Object myFloatUnsigned = row.get("myFloatUnsigned");
        assertNotNull(myFloatUnsigned, "myFloatUnsigned");
        assertTrue(myFloatUnsigned instanceof Double, "myFloatUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myFloatUnsigned"), MySQLType.FLOAT_UNSIGNED, "myFloatUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myFloatUnsigned"), "myFloatUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloatUnsigned"), NullMode.NON_NULL, "myFloatUnsigned null mode.");


        final Object myDouble = row.get("myDouble");
        assertNotNull(myDouble, "myDouble");
        assertTrue(myDouble instanceof Double, "myDouble java class.");
        assertEquals(rowMeta.getSQLType("myDouble"), MySQLType.DOUBLE, "myDouble mysql type");
        assertFalse(rowMeta.isUnsigned("myDouble"), "myDouble isUnsigned");
        assertEquals(rowMeta.getNullMode("myDouble"), NullMode.NON_NULL, "myDouble null mode.");

        final Object myDoubleUnsigned = row.get("myDoubleUnsigned");
        assertNotNull(myDoubleUnsigned, "myDoubleUnsigned");
        assertTrue(myDoubleUnsigned instanceof BigDecimal, "myDoubleUnsigned java class.");
        assertEquals(rowMeta.getSQLType("myDoubleUnsigned"), MySQLType.DOUBLE_UNSIGNED, "myDoubleUnsigned mysql type");
        assertTrue(rowMeta.isUnsigned("myDoubleUnsigned"), "myDoubleUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myDoubleUnsigned"), NullMode.NON_NULL, "myDoubleUnsigned null mode.");


        final Object myEnum = row.get("myEnum");
        assertNotNull(myEnum, "myEnum");
        assertTrue(myEnum instanceof String, "myEnum java class.");
        final TrueOrFalse trueOrFalse = row.get("myEnum", TrueOrFalse.class);
        assertNotNull(trueOrFalse, "myEnum");
        assertEquals(rowMeta.getSQLType("myEnum"), MySQLType.ENUM, "myEnum mysql type");
        assertFalse(rowMeta.isUnsigned("myEnum"), "myEnum isUnsigned");
        assertEquals(rowMeta.getNullMode("myEnum"), NullMode.NON_NULL, "myEnum null mode.");

        // set type assert start

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
        // set type assert end


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
                assertTrue(row.get(columnAlias) instanceof Byte, columnAlias + " is Byte type.");
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
            MySQLSessionAdjutant sessionAdjutant = getSessionAdjutantForSingleHost(Collections.emptyMap());
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
        final int rowCount = 10;

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
