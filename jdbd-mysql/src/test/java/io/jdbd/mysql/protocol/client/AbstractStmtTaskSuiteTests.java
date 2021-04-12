package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.*;
import io.jdbd.vendor.util.Geometries;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.testng.Assert.*;

public abstract class AbstractStmtTaskSuiteTests extends AbstractConnectionBasedSuiteTests {


    abstract Mono<ResultStates> executeUpdate(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant);

    abstract Flux<ResultRow> executeQuery(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant);

    private final SubType subType;

    protected AbstractStmtTaskSuiteTests(SubType subType) {
        this.subType = subType;
    }

    final void doBigIntBindAndExtract(Logger LOG) {
        LOG.info("bigIntBindAndExtract test start");

        final String sql = "SELECT t.id as id, t.create_time as createTime FROM mysql_types as t WHERE t.id  = ?";
        final long id = convertId(2);
        BindValue bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, id);
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        List<ResultRow> resultRowList;
        resultRowList = ComQueryTask.bindableQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        Long resultId = resultRow.get("id", Long.class);

        assertEquals(resultId, Long.valueOf(id), "id");

        // string bigint
        bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, Long.toString(id));
        resultRowList = ComQueryTask.bindableQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        resultId = resultRow.get("id", Long.class);

        assertEquals(resultId, Long.valueOf(id), "id");

        LOG.info("bigIntBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doDateBindAndExtract(Logger LOG) {
        LOG.info("doDateBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertDateBindAndExtract(taskAdjutant, LocalDate.now());
        assertDateBindAndExtract(taskAdjutant, "2021-04-10");

        LOG.info("doDateBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTimeBindAndExtract(Logger LOG) {
        LOG.info("doTimeBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String field;

        field = "my_time";

        assertTimeBindAndExtract(taskAdjutant, LocalTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, OffsetTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, "19:26:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "00:00:00", field);

        assertTimeBindAndExtract(taskAdjutant, "00:00:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59.999999", field);

        assertTimeBindAndExtract(taskAdjutant, Duration.ZERO, field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimeUtils.parseTimeAsDuration("838:59:59"), field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimeUtils.parseTimeAsDuration("-838:59:59.000000"), field);


        field = "my_time1";

        assertTimeBindAndExtract(taskAdjutant, LocalTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, OffsetTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, "19:26:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "00:00:00", field);

        assertTimeBindAndExtract(taskAdjutant, "00:00:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59.999999", field);

        assertTimeBindAndExtract(taskAdjutant, Duration.ZERO, field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimeUtils.parseTimeAsDuration("838:59:59"), field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimeUtils.parseTimeAsDuration("-838:59:59.000000"), field);


        LOG.info("doTimeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doDatetimeBindAndExtract(Logger LOG) {
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

    final void doStringBindAndExtract(Logger LOG) {
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

    final void doBinaryBindAndExtract(Logger LOG) {
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


    final void doBitBindAndExtract(Logger LOG) {
        LOG.info("bitBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String field;

        assertBitBindAndExtract(taskAdjutant, -1L, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Long.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Long.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, 0L, "my_bit");

        assertBitBindAndExtract(taskAdjutant, -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Integer.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Integer.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, (short) -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Short.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Short.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, (short) 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, (byte) -1, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Byte.MAX_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, Byte.MIN_VALUE, "my_bit");
        assertBitBindAndExtract(taskAdjutant, (byte) 0, "my_bit");

        assertBitBindAndExtract(taskAdjutant, "101001010010101", "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLNumberUtils.longToBigEndianBytes(-1L), "my_bit");
        assertBitBindAndExtract(taskAdjutant, MySQLNumberUtils.longToBigEndianBytes(0L), "my_bit");


        field = "my_bit20";
        assertBitBindAndExtract(taskAdjutant, Long.parseLong("10101101001101", 2), field);
        assertBitBindAndExtract(taskAdjutant, 0B1111_1111_1111_1111_1111L, field);
        assertBitBindAndExtract(taskAdjutant, 0L, field);

        assertBitBindAndExtract(taskAdjutant, MySQLNumberUtils.longToBigEndianBytes(0B0000_0000_0000_0000L), field);

        LOG.info("bitBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doTinyint1BindExtract(Logger LOG) {
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

    final void doNumberBindAndExtract(Logger LOG) {
        LOG.info("numberBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String field = "my_tinyint";
        MySQLType mySQLType = MySQLType.TINYINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_tinyint_unsigned";
        mySQLType = MySQLType.TINYINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_smallint";
        mySQLType = MySQLType.SMALLINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_smallint_unsigned";
        mySQLType = MySQLType.SMALLINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_mediumint";
        mySQLType = MySQLType.MEDIUMINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -0x7FFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0x7FFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_mediumint_unsigned";
        mySQLType = MySQLType.MEDIUMINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_int";
        mySQLType = MySQLType.INT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_int_unsigned";
        mySQLType = MySQLType.INT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FFFFL, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_bigint";
        mySQLType = MySQLType.BIGINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_bigint_unsigned";
        mySQLType = MySQLType.BIGINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, MySQLNumberUtils.MAX_UNSIGNED_LONG, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_decimal";
        mySQLType = MySQLType.DECIMAL;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("-34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_decimal_unsigned";
        mySQLType = MySQLType.DECIMAL_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.1"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);


        field = "my_float";
        mySQLType = MySQLType.FLOAT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);

        field = "my_float_unsigned";
        mySQLType = MySQLType.FLOAT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field);

        field = "my_double";
        mySQLType = MySQLType.DOUBLE;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);

        field = "my_double_unsigned";
        mySQLType = MySQLType.DOUBLE_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field);


        LOG.info("numberBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doEnumBindAndExtract(Logger LOG) {
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


    final void doSetTypeBindAndExtract(Logger LOG) {
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

    final void doJsonBindAndExtract(Logger LOG) throws Exception {
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


    final void doTinyBlobBindExtract(Logger LOG) {
        LOG.info("doTinyBlobBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTinyBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTinyBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doTinyBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doBlobBindExtract(Logger LOG) {
        LOG.info("doBlobBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMediumBlobBindExtract(Logger LOG) {
        LOG.info("doMediumBlobBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertMediumBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertMediumBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doMediumBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doLongBlobBindExtract(Logger LOG) {
        LOG.info("doLongBlobBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertLongBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertLongBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doLongBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTinyTextBindAndExtract(Logger LOG) {
        LOG.info("doTinyTextBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTinyTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTinyTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyTextBindAndExtract(taskAdjutant, array);

        LOG.info("doTinyTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTextBindAndExtract(Logger LOG) {
        LOG.info("doTextBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTextBindAndExtract(taskAdjutant, array);

        LOG.info("doTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doMediumTextBindAndExtract(Logger LOG) {
        LOG.info("doMediumTextBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertMediumTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertMediumTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumTextBindAndExtract(taskAdjutant, array);

        LOG.info("doMediumTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doLongTextBindAndExtract(Logger LOG) {
        LOG.info("doLongTextBindAndExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.obtainCharsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertLongTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertLongTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongTextBindAndExtract(taskAdjutant, array);

        LOG.info("doLongTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doGeometryBindAndExtract(Logger LOG) {
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

        //MySQL 8.0.23 bug ,can't parse GEOMETRY_COLLECTION with big endian . @see https://bugs.mysql.com/bug.php?id=103262&thanks=4
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("geometryBindAndExtract test success");
        releaseConnection(taskAdjutant);

    }


    final void doPointBindAndExtract(Logger LOG) {
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


    final void doLineStringBindAndExtract(Logger LOG) {
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


    final void doPolygonBindAndExtract(Logger LOG) {
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


    final void doMultiPointBindExtract(Logger LOG) {
        LOG.info("multiPointBindExtract test start");
        final MySQLTaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("MULTIPOINT(( 0 0 ),(1 1),(1 3), (0 0),(%s %s)) "
                , Double.MIN_VALUE, Double.MAX_VALUE);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POINT with big endian . @see https://bugs.mysql.com/bug.php?id=103262&thanks=4
//        wkbArray = Geometries.multiPointToWkb(wktText, true);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = Geometries.multiPointToWkb(wktText, false);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiPointBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMultiLineStringBindExtract(Logger LOG) {
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


    final void doMultiPolygonBindExtract(Logger LOG) {
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


    final void doGeometryCollectionBindExtract(Logger LOG) {
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
     * @see #doGeometryBindAndExtract(Logger)
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
     * @see #doPointBindAndExtract(Logger)
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
     * @see #doLineStringBindAndExtract(Logger)
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
     * @see #doPolygonBindAndExtract(Logger)
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
     * @see #doMultiPointBindExtract(Logger)
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
     * @see #doMultiLineStringBindExtract(Logger)
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
     * @see #doMultiPolygonBindExtract(Logger)
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
     * @see #doGeometryCollectionBindExtract(Logger)
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
     * @see #doDateBindAndExtract(Logger)
     */
    private void assertDateBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(19);
        final String field = "my_date";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.DATE, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final LocalDate resultDate = resultRow.getNonNull("field", LocalDate.class);

        final LocalDate bindDate;
        if (bindParam instanceof String) {
            bindDate = LocalDate.parse((String) bindParam);
        } else {
            bindDate = (LocalDate) bindParam;
        }
        assertEquals(resultDate, bindDate, field);
    }

    /**
     * @see #doTimeBindAndExtract(Logger)
     */
    private void assertTimeBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam
            , final String field) {
        final long id = convertId(20);
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TIME, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);


        if (bindParam instanceof Duration) {
            final Duration resultDuration = resultRow.getNonNull("field", Duration.class);
            assertEquals(resultDuration, bindParam, field);
            return;
        }

        final LocalTime bindTime, resultTime = resultRow.getNonNull("field", LocalTime.class);
        if (bindParam instanceof String) {
            bindTime = LocalTime.parse((String) bindParam, MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        } else if (bindParam instanceof OffsetTime) {
            bindTime = ((OffsetTime) bindParam).withOffsetSameInstant(taskAdjutant.obtainZoneOffsetClient())
                    .toLocalTime();
        } else {
            bindTime = (LocalTime) bindParam;
        }
        final DateTimeFormatter formatter = MySQLTimeUtils.obtainTimeFormatter(
                (int) resultRow.getRowMeta().getPrecision("field"));

        final LocalTime time = LocalTime.parse(bindTime.format(formatter), MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        if (taskAdjutant.obtainHostInfo().getProperties()
                .getOrDefault(PropertyKey.timeTruncateFractional, Boolean.class)) {
            assertEquals(resultTime, time, field);
        } else {
            Duration duration = Duration.between(time, resultTime);
            assertFalse(duration.isNegative(), field);
            assertTrue(duration.getSeconds() < 1L, field);
        }

    }


    /**
     * @see #doTinyBlobBindExtract(Logger)
     */
    private void assertTinyBlobBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(21);
        final String field = "my_tinyblob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TINYBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.obtainCharsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doBlobBindExtract(Logger)
     */
    private void assertBlobBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(22);
        final String field = "my_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.BLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.obtainCharsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doMediumBlobBindExtract(Logger)
     */
    private void assertMediumBlobBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(23);
        final String field = "my_medium_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.MEDIUMBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.obtainCharsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doLongBlobBindExtract(Logger)
     */
    private void assertLongBlobBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(24);
        final String field = "my_long_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.LONGBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.obtainCharsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doTinyTextBindAndExtract(Logger)
     */
    private void assertTinyTextBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(25);
        final String field = "my_tiny_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TINYTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.obtainCharsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doTextBindAndExtract(Logger)
     */
    private void assertTextBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(26);
        final String field = "my_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.obtainCharsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doMediumTextBindAndExtract(Logger)
     */
    private void assertMediumTextBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(27);
        final String field = "my_medium_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.MEDIUMTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.obtainCharsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doLongTextBindAndExtract(Logger)
     */
    private void assertLongTextBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(28);
        final String field = "my_long_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.LONGTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.obtainCharsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }


    /**
     * @see #assertSetTypeBindAndExtract(MySQLTaskAdjutant, MySQLType, Object)
     */
    private void assertSetTypeBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(9);
        final String field = "my_set";

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

    /**
     * @see #doEnumBindAndExtract(Logger)
     */
    private void assertEnumBindExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(8);
        final String field = "my_enum";
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
     * @see #doNumberBindAndExtract(Logger)
     */
    private void assertNumberBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field) {
        final long id = convertId(7);
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
     * @see #doTinyint1BindExtract(Logger)
     */
    private void assertTinyInt1BindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(6);
        final String field = "my_tinyint1";
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
     * @see #doBitBindAndExtract(Logger)
     */
    private void assertBitBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final Object bindParam
            , final String field) {
        final long id = convertId(5);
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.BIT, bindParam, field, id);
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
     * @see #doStringBindAndExtract(Logger)
     */
    private void assertBinaryBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final long id = convertId(4);
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
     * @see #doStringBindAndExtract(Logger)
     */
    private void assertStringBindAndExtract(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final long id = convertId(3);

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
     * @see #doDatetimeBindAndExtract(Logger)
     */
    private void assertDateTimeModify(final MySQLTaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindDatetime, final String field) {
        final long id = convertId(2);
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

        io.jdbd.vendor.conf.Properties<PropertyKey> properties = taskAdjutant.obtainHostInfo().getProperties();
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
            , final Object bindParam, final String field, final Object id) {

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
        resultStates = executeUpdate(StmtWrappers.multi(sql, bindValueList), taskAdjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "getAffectedRows");
    }

    private ResultRow querySingleField(final MySQLTaskAdjutant taskAdjutant, final String field, final Object id) {
        String sql = String.format("SELECT t.id as id, t.%s as field FROM mysql_types as t WHERE t.id = ?", field);
        BindValue bindValue = MySQLBindValue.create(0, MySQLType.BIGINT, id);

        List<ResultRow> resultRowList;
        resultRowList = executeQuery(StmtWrappers.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();
        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 1L, "resultRowList size");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        return resultRow;
    }


    private long convertId(final long id) {
        final long newId;
        switch (this.subType) {
            case COM_QUERY:
                newId = id;
                break;
            case COM_PREPARE_STMT:
                newId = id + 50L;
                break;
            case PREPARE:
                newId = id + 100L;
                break;
            case BINDABLE:
                newId = id + 150L;
                break;
            default:
                throw MySQLExceptions.createUnknownEnumException(this.subType);

        }
        return newId;
    }


    protected enum SubType {
        COM_QUERY,
        COM_PREPARE_STMT,
        BINDABLE,
        PREPARE

    }


}