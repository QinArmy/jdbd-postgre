package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is base class of :
 * <u>
 * <li>{@link SimpleQuerySqlTypeSuiteTests}</li>
 * <li>{@link ExtendedQueryTaskSqlTypeSuiteTests}</li>
 * </u>
 * </p>
 */
abstract class AbstractStmtTaskTests extends AbstractTaskTests {


    private final int startId;

    AbstractStmtTaskTests(int startId) {
        this.startId = startId;
    }

    abstract Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant);


    /**
     * @see PgType#SMALLINT
     */
    final void doSmallIntBindAndExtract() {
        final String columnName = "my_smallint";
        final long id = startId + 1;

        testType(columnName, PgType.SMALLINT, null, id);

        testType(columnName, PgType.SMALLINT, Short.MAX_VALUE, id);
        testType(columnName, PgType.SMALLINT, Short.MIN_VALUE, id);
        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.SMALLINT, (byte) 0, id);

        testType(columnName, PgType.SMALLINT, 0, id);
        testType(columnName, PgType.SMALLINT, 0L, id);
        testType(columnName, PgType.SMALLINT, BigDecimal.valueOf(Short.MAX_VALUE), id);
        testType(columnName, PgType.SMALLINT, BigInteger.valueOf(Short.MIN_VALUE), id);

        testType(columnName, PgType.SMALLINT, Short.toString(Short.MAX_VALUE), id);
        testType(columnName, PgType.SMALLINT, Boolean.TRUE, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);

        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Short.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Short.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Short.toString(Short.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Short.toString(Short.MIN_VALUE), id);

    }

    /**
     * @see PgType#INTEGER
     */
    final void doIntegerBindAndExtract() {
        final String columnName = "my_integer";
        final long id = startId + 2;

        testType(columnName, PgType.INTEGER, null, id);

        testType(columnName, PgType.INTEGER, Integer.MAX_VALUE, id);
        testType(columnName, PgType.INTEGER, Integer.MIN_VALUE, id);
        testType(columnName, PgType.INTEGER, (short) 0, id);
        testType(columnName, PgType.INTEGER, (byte) 0, id);

        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.INTEGER, 0L, id);
        testType(columnName, PgType.INTEGER, BigDecimal.valueOf(Integer.MAX_VALUE), id);
        testType(columnName, PgType.INTEGER, BigInteger.valueOf(Integer.MIN_VALUE), id);

        testType(columnName, PgType.INTEGER, Integer.toString(Integer.MAX_VALUE), id);
        testType(columnName, PgType.INTEGER, Boolean.TRUE, id);
        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);

        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Integer.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Integer.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Integer.toString(Integer.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Integer.toString(Integer.MIN_VALUE), id);
    }


    /**
     * @see PgType#BIGINT
     */
    final void doBigintBindAndExtract() {
        final String columnName = "my_bigint";
        final long id = startId + 3;

        testType(columnName, PgType.BIGINT, null, id);

        testType(columnName, PgType.BIGINT, Long.MAX_VALUE, id);
        testType(columnName, PgType.BIGINT, Long.MIN_VALUE, id);
        testType(columnName, PgType.BIGINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, (byte) 0, id);

        testType(columnName, PgType.BIGINT, 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.BIGINT, BigDecimal.valueOf(Long.MAX_VALUE), id);
        testType(columnName, PgType.BIGINT, BigInteger.valueOf(Long.MIN_VALUE), id);

        testType(columnName, PgType.BIGINT, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.BIGINT, Boolean.TRUE, id);

        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);

        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MIN_VALUE), id);
    }

    /**
     * @see PgType#DECIMAL
     */
    final void doDecimalBindAndExtract() {
        final String columnName = "my_decimal";
        final long id = startId + 4;

        testType(columnName, PgType.DECIMAL, null, id);

        testType(columnName, PgType.DECIMAL, Long.MAX_VALUE, id);
        testType(columnName, PgType.DECIMAL, Long.MIN_VALUE, id);
        testType(columnName, PgType.DECIMAL, (short) 0, id);
        testType(columnName, PgType.DECIMAL, (byte) 0, id);

        testType(columnName, PgType.DECIMAL, 0, id);
        testType(columnName, PgType.DECIMAL, 0L, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);

        testType(columnName, PgType.DECIMAL, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, Boolean.TRUE, id);

        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);

        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MIN_VALUE), id);
    }

    /**
     * @see PgType#REAL
     */
    final void doRealBindAndExtract() {
        final String columnName = "my_real";
        final long id = startId + 5;

        testType(columnName, PgType.REAL, null, id);

        testType(columnName, PgType.REAL, Float.MAX_VALUE, id);
        testType(columnName, PgType.REAL, Float.MIN_VALUE, id);
        testType(columnName, PgType.REAL, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.REAL, Float.toString(Float.MAX_VALUE), id);

        testType(columnName, PgType.REAL, 0.0F, id);
        testType(columnName, PgType.REAL, Boolean.TRUE, id);
        testType(columnName, PgType.REAL, Boolean.FALSE, id);

        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);

    }

    /**
     * @see PgType#DOUBLE
     */
    final void doDoubleBindAndExtract() {
        final String columnName = "my_double";
        final long id = startId + 6;

        testType(columnName, PgType.DOUBLE, null, id);

        testType(columnName, PgType.DOUBLE, Double.MAX_VALUE, id);
        testType(columnName, PgType.DOUBLE, Double.MIN_VALUE, id);
        testType(columnName, PgType.DOUBLE, Double.toString(Double.MAX_VALUE), id);
        testType(columnName, PgType.DOUBLE, Double.toString(Double.MAX_VALUE), id);

        testType(columnName, PgType.DOUBLE, 0.0D, id);
        testType(columnName, PgType.DOUBLE, Boolean.TRUE, id);
        testType(columnName, PgType.DOUBLE, Boolean.FALSE, id);

        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Double.toString(Double.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Double.toString(Double.MAX_VALUE), id);

    }

    /**
     * @see PgType#BOOLEAN
     */
    final void doBooleanBindAndExtract() {
        final String columnName = "my_boolean";
        final long id = startId + 7;

        testType(columnName, PgType.BOOLEAN, null, id);

        testType(columnName, PgType.BOOLEAN, true, id);
        testType(columnName, PgType.BOOLEAN, false, id);
        testType(columnName, PgType.BOOLEAN, (byte) 0, id);
        testType(columnName, PgType.BOOLEAN, (byte) 1, id);

        testType(columnName, PgType.BOOLEAN, (short) 0, id);
        testType(columnName, PgType.BOOLEAN, (short) 1, id);
        testType(columnName, PgType.BOOLEAN, 0, id);
        testType(columnName, PgType.BOOLEAN, 1, id);

        testType(columnName, PgType.BOOLEAN, 0L, id);
        testType(columnName, PgType.BOOLEAN, 1L, id);
        testType(columnName, PgType.BOOLEAN, 0.0F, id);
        testType(columnName, PgType.BOOLEAN, 1.0F, id);

        testType(columnName, PgType.BOOLEAN, 0.0D, id);
        testType(columnName, PgType.BOOLEAN, 1.0D, id);
        testType(columnName, PgType.BOOLEAN, BigInteger.ZERO, id);
        testType(columnName, PgType.BOOLEAN, BigInteger.ONE, id);

        testType(columnName, PgType.BOOLEAN, BigDecimal.ZERO, id);
        testType(columnName, PgType.BOOLEAN, BigDecimal.ONE, id);
        testType(columnName, PgType.BOOLEAN, PgConstant.TRUE, id);
        testType(columnName, PgType.BOOLEAN, PgConstant.FALSE, id);

        testType(columnName, PgType.BOOLEAN, "T", id);
        testType(columnName, PgType.BOOLEAN, "F", id);
        testType(columnName, PgType.VARCHAR, PgConstant.TRUE, id);
        testType(columnName, PgType.VARCHAR, PgConstant.FALSE, id);

        testType(columnName, PgType.VARCHAR, "T", id);
        testType(columnName, PgType.VARCHAR, "F", id);

    }

    /**
     * @see PgType#TIMESTAMP
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doTimestampBindExtract() {
        final String columnName = "my_timestamp";
        final long id = startId + 8;

        final DateTimeFormatter iso = PgTimes.ISO_LOCAL_DATETIME_FORMATTER, pgIso = PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER;

        testType(columnName, PgType.TIMESTAMP, null, id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso), id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 00:00:00.999999 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 23:59:59.999999 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 00:00:00.999999 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 23:59:59.999999 BC", pgIso), id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-01-01 00:00:00 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-01-01 23:59:59 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-12-31 00:00:00 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-12-31 23:59:59 AD", pgIso), id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso).format(iso), id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-01-01 00:00:00.999999 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-01-01 23:59:59.999999 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-12-31 00:00:00.999999 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("294276-12-31 23:59:59.999999 AD", pgIso).format(iso), id);

        testType(columnName, PgType.TIMESTAMP, LocalDateTime.now(), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.now().format(iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("0000-01-01 00:00:00", iso), id);
        testType(columnName, PgType.TIMESTAMP, LocalDateTime.parse("0000-01-01 23:59:59", iso), id);

        testType(columnName, PgType.TIMESTAMP, "0000-01-01 00:00:00", id);
        testType(columnName, PgType.TIMESTAMP, "0000-01-01 23:59:59", id);
        testType(columnName, PgType.TIMESTAMP, "infinity", id);
        testType(columnName, PgType.TIMESTAMP, "-infinity", id);

        testType(columnName, PgType.TIMESTAMP, "INFINITY", id);
        testType(columnName, PgType.TIMESTAMP, "-INFINITY", id);

    }

    /**
     * @see PgType#TIMESTAMPTZ
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doTimestampTzBindExtract() {
        final String columnName = "my_zoned_timestamp";
        final long id = startId + 9;

        final DateTimeFormatter iso = PgTimes.ISO_OFFSET_DATETIME_FORMATTER, pgIso = PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER;

        testType(columnName, PgType.TIMESTAMPTZ, null, id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso), id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 00:00:00.999999+00:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 23:59:59.999999+00:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 00:00:00.999999+00:00 BC", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 23:59:59.999999+00:00 BC", pgIso), id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-01-01 00:00:00+08:00 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-01-01 23:59:59+08:00 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-12-31 00:00:00+08:00 AD", pgIso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-12-31 23:59:59+08:00 AD", pgIso), id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso).format(iso), id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-01-01 00:00:00.999999+00:00 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-01-01 23:59:59.999999+00:00 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-12-31 00:00:00.999999+00:00 AD", pgIso).format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("294276-12-31 23:59:59.999999+00:00 AD", pgIso).format(iso), id);

        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.now(), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.now().format(iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("0000-01-01 00:00:00+00:00", iso), id);
        testType(columnName, PgType.TIMESTAMPTZ, OffsetDateTime.parse("0000-01-01 23:59:59+00:00", iso), id);

        testType(columnName, PgType.TIMESTAMPTZ, "0000-01-01 00:00:00+08:00", id);
        testType(columnName, PgType.TIMESTAMPTZ, "0000-01-01 23:59:59+08:00", id);
        testType(columnName, PgType.TIMESTAMPTZ, "infinity", id);
        testType(columnName, PgType.TIMESTAMPTZ, "-infinity", id);

        testType(columnName, PgType.TIMESTAMPTZ, "INFINITY", id);
        testType(columnName, PgType.TIMESTAMPTZ, "-INFINITY", id);
    }

    /**
     * @see PgType#DATE
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doDateBindAndExtract() {
        final String columnName = "my_date";
        final long id = startId + 10;

        final DateTimeFormatter iso = DateTimeFormatter.ISO_DATE, pgIso = PgTimes.PG_ISO_LOCAL_DATE_FORMATTER;

        testType(columnName, PgType.DATE, null, id);

        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso), id);

        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso), id);

        testType(columnName, PgType.DATE, LocalDate.parse("5874897-01-01 AD", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-01-01 AD", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-12-31 AD", pgIso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-12-31 AD", pgIso), id);

        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-01-01 BC", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("4713-12-31 BC", pgIso).format(iso), id);

        testType(columnName, PgType.DATE, LocalDate.parse("5874897-01-01 AD", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-01-01 AD", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-12-31 AD", pgIso).format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("5874897-12-31 AD", pgIso).format(iso), id);

        testType(columnName, PgType.DATE, LocalDate.now(), id);
        testType(columnName, PgType.DATE, LocalDate.now().format(iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("0000-01-01", iso), id);
        testType(columnName, PgType.DATE, LocalDate.parse("0000-01-01", iso), id);

        testType(columnName, PgType.DATE, "0000-01-01", id);
        testType(columnName, PgType.DATE, "0000-01-01", id);
        testType(columnName, PgType.DATE, "infinity", id);
        testType(columnName, PgType.DATE, "-infinity", id);

        testType(columnName, PgType.DATE, "INFINITY", id);
        testType(columnName, PgType.DATE, "-INFINITY", id);
    }

    /**
     * @see PgType#TIME
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doTimeBindAndExtract() {
        final String columnName = "my_time";
        final long id = startId + 11;

        final DateTimeFormatter iso = PgTimes.ISO_LOCAL_TIME_FORMATTER;

        testType(columnName, PgType.TIME, null, id);

        testType(columnName, PgType.TIME, LocalTime.MAX, id);
        testType(columnName, PgType.TIME, LocalTime.MIN, id);
        testType(columnName, PgType.TIME, LocalTime.MIDNIGHT, id);
        testType(columnName, PgType.TIME, LocalTime.now(), id);

        testType(columnName, PgType.TIME, LocalTime.now().format(iso), id);

        testType(columnName, PgType.TIME, "23:59:59", id);
        testType(columnName, PgType.TIME, "00:00:00", id);
        testType(columnName, PgType.TIME, "23:59:59.999999", id);
        testType(columnName, PgType.TIME, "00:00:00.999999", id);
    }


    private void testType(final String columnName, final PgType columnType
            , final @Nullable Object value, final long id) {
        assertNotEquals(columnName, "id", "can't be id");

        //1. update column
        final String updateSql = String.format("UPDATE my_types AS t SET %s = ? WHERE t.id = ?", columnName);

        final List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, columnType, value));
        paramGroup.add(BindValue.wrap(1, PgType.BIGINT, id));

        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final ResultStates states;
        states = executeUpdate(PgStmts.bind(updateSql, paramGroup), adjutant)

                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states, 0);

        // 2. query column
        final String querySql = String.format("SELECT t.id AS \"id\", t.%s \"%s\" FROM my_types AS t WHERE t.id = ?", columnName, columnName);

        final ResultRow row;
        row = executeQuery(PgStmts.bind(querySql, BindValue.wrap(0, PgType.BIGINT, id)), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .map(this::mapToSingleton)
                .block();

        // 3. assert update result

        assertNotNull(row, "row");
        assertEquals(row.get("id"), id, "id");

        if (value == null) {
            assertNull(row.get(columnName), columnName);
        } else {
            assertTestResult(columnName, row, value);
        }
    }


    /**
     * @see #testType(String, PgType, Object, long)
     */
    private <T> T mapToSingleton(List<T> singletonList) {
        assertEquals(singletonList.size(), 1, "list size");
        return singletonList.get(0);
    }

    /**
     * @see #testType(String, PgType, Object, long)
     */
    private void assertTestResult(final String columnName, final ResultRow row, final Object nonNull) {
        final SQLType sqlType = row.getRowMeta().getSQLType(columnName);
        final Class<?> javaType = sqlType.javaType();
        if (javaType == BigDecimal.class && nonNull instanceof String) {
            final BigDecimal v = new BigDecimal((String) nonNull);
            final BigDecimal result = row.getNonNull(columnName, BigDecimal.class);
            assertEquals(result.compareTo(v), 0, columnName);
        } else if (javaType == Double.class && nonNull instanceof String) {
            final double v = Double.parseDouble((String) nonNull);
            assertEquals(row.get(columnName, Double.class), Double.valueOf(v), columnName);
        } else if (javaType == Float.class && nonNull instanceof String) {
            final float v = Float.parseFloat((String) nonNull);
            assertEquals(row.get(columnName, Float.class), Float.valueOf(v), columnName);
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = (BigDecimal) nonNull;
            final BigDecimal result = row.getNonNull(columnName, BigDecimal.class);
            assertEquals(result.compareTo(v), 0, columnName);
        } else if (javaType == Boolean.class && nonNull instanceof String) {
            final String v = (String) nonNull;
            final boolean bindValue;
            if (v.equalsIgnoreCase(PgConstant.TRUE)
                    || v.equalsIgnoreCase("T")) {
                bindValue = true;
            } else if (v.equalsIgnoreCase(PgConstant.FALSE)
                    || v.equalsIgnoreCase("F")) {
                bindValue = false;
            } else {
                throw new IllegalArgumentException(String.format("value[%s] error.", nonNull));
            }
            assertEquals(row.get(columnName, Boolean.class), Boolean.valueOf(bindValue), columnName);
        } else if (javaType == LocalDate.class) {
            final LocalDate v;
            if (nonNull instanceof String) {
                if (assertDateOrTimestampSpecial(columnName, row, nonNull)) {
                    return;
                }
                v = LocalDate.parse((String) nonNull, DateTimeFormatter.ISO_LOCAL_DATE);
            } else {
                v = (LocalDate) nonNull;
            }
            assertEquals(row.get(columnName, LocalDate.class), v, columnName);
        } else if (javaType == LocalTime.class) {
            final LocalTime v;
            if (nonNull instanceof String) {
                v = LocalTime.parse((String) nonNull, PgTimes.ISO_LOCAL_TIME_FORMATTER);
            } else {
                v = (LocalTime) nonNull;
            }
            // database possibly round
            final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, LocalTime.class));
            assertTrue(intervalMicro == 0 || intervalMicro == 1, columnName);
        } else if (javaType == OffsetTime.class) {
            final OffsetTime v;
            if (nonNull instanceof String) {
                v = OffsetTime.parse((String) nonNull, PgTimes.ISO_OFFSET_TIME_FORMATTER);
            } else {
                v = (OffsetTime) nonNull;
            }
            // database possibly round
            final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, OffsetTime.class));
            assertTrue(intervalMicro == 0 || intervalMicro == 1, columnName);
        } else if (javaType == LocalDateTime.class) {
            final LocalDateTime v;
            if (nonNull instanceof String) {
                if (assertDateOrTimestampSpecial(columnName, row, nonNull)) {
                    return;
                }
                v = LocalDateTime.parse((String) nonNull, PgTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } else {
                v = (LocalDateTime) nonNull;
            }
            // database possibly round
            final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, LocalDateTime.class));
            assertTrue(intervalMicro == 0 || intervalMicro == 1, columnName);
        } else if (javaType == OffsetDateTime.class) {
            final OffsetDateTime v;
            if (nonNull instanceof String) {
                if (assertDateOrTimestampSpecial(columnName, row, nonNull)) {
                    return;
                }
                v = OffsetDateTime.parse((String) nonNull, PgTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                v = (OffsetDateTime) nonNull;
            }
            // database possibly round
            final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, OffsetDateTime.class));
            assertTrue(intervalMicro == 0 || intervalMicro == 1, columnName);
        } else {
            assertEquals(row.get(columnName, nonNull.getClass()), nonNull, columnName);
        }
    }

    private boolean assertDateOrTimestampSpecial(final String columnName, final ResultRow row, final Object nonNull) {
        final String textValue = ((String) nonNull).toLowerCase();
        switch (textValue) {
            case PgConstant.INFINITY:
            case PgConstant.NEG_INFINITY: {
                final String resultText = row.get(columnName, String.class);
                assertNotNull(resultText, columnName);
                assertEquals(resultText.toLowerCase(), textValue, columnName);
                return true;
            }
        }
        return false;
    }


}
