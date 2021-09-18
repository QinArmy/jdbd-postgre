package io.jdbd.postgre.protocol.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.type.*;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.type.Interval;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

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

    final Logger log = LoggerFactory.getLogger(getClass());


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
        testType(columnName, PgType.TIMESTAMPTZ, ZonedDateTime.now(), id);
        testType(columnName, PgType.TIMESTAMPTZ, ZonedDateTime.now().format(iso), id);

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
        testType(columnName, PgType.TIME, LocalTime.NOON, id);
        testType(columnName, PgType.TIME, LocalTime.now(), id);

        testType(columnName, PgType.TIME, LocalTime.now().format(iso), id);

        testType(columnName, PgType.TIME, "23:59:59", id);
        testType(columnName, PgType.TIME, "00:00:00", id);
        testType(columnName, PgType.TIME, "23:59:59.999999", id);
        testType(columnName, PgType.TIME, "00:00:00.999999", id);
    }

    /**
     * @see PgType#TIMETZ
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doTimeTzBindAndExtract() {
        final String columnName = "my_zoned_time";
        final long id = startId + 12;

        final DateTimeFormatter iso = PgTimes.ISO_OFFSET_TIME_FORMATTER;

        testType(columnName, PgType.TIMETZ, null, id);

        testType(columnName, PgType.TIMETZ, OffsetTime.of(LocalTime.MAX, ZoneOffset.of("+08:00")), id);
        testType(columnName, PgType.TIMETZ, OffsetTime.of(LocalTime.MIN, ZoneOffset.of("+08:00")), id);
        testType(columnName, PgType.TIMETZ, OffsetTime.of(LocalTime.NOON, ZoneOffset.of("+08:00")), id);
        testType(columnName, PgType.TIMETZ, OffsetTime.now(), id);

        testType(columnName, PgType.TIMETZ, OffsetTime.now().format(iso), id);
        testType(columnName, PgType.TIMETZ, "23:59:59+08:00", id);
        testType(columnName, PgType.TIMETZ, "00:00:00+08:00", id);
        testType(columnName, PgType.TIMETZ, "23:59:59.999999+08:00", id);

        testType(columnName, PgType.TIMETZ, "00:00:00.999999+08:00", id);
    }

    /**
     * @see PgType#BIT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-bit.html">Bit String Types</a>
     */
    final void doBit64BindAndExtract() {
        final String columnName = "my_bit64";
        final long id = startId + 13;
        // bit type is fixed length.
        final char[] bitChars = new char[64];
        Arrays.fill(bitChars, '0');
        final String allZeroBits = new String(bitChars);
        Arrays.fill(bitChars, '1');
        final String allOneBits = new String(bitChars);


        testType(columnName, PgType.BIT, null, id);

        testType(columnName, PgType.BIT, allZeroBits, id);
        testType(columnName, PgType.BIT, allOneBits, id);
        testType(columnName, PgType.BIT, -1L, id);
        testType(columnName, PgType.BIT, 0L, id);

        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            testType(columnName, PgType.BIT, random.nextLong(), id);
        }


        for (int i = 0; i < 10; i++) {
            BitSet bitSet = BitSet.valueOf(new long[]{random.nextLong()});
            if (!bitSet.get(63)) {// bit type is fixed length.
                bitSet.set(63);
            }
            testType(columnName, PgType.BIT, bitSet, id);
        }

    }

    /**
     * @see PgType#BIT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-bit.html">Bit String Types</a>
     */
    final void doBit32BindAndExtract() {
        final String columnName = "my_bit32";
        final long id = startId + 14;
        // bit type is fixed length.
        final char[] bitChars = new char[32];
        Arrays.fill(bitChars, '0');
        final String allZeroBits = new String(bitChars);
        Arrays.fill(bitChars, '1');
        final String allOneBits = new String(bitChars);


        testType(columnName, PgType.BIT, null, id);

        testType(columnName, PgType.BIT, allZeroBits, id);
        testType(columnName, PgType.BIT, allOneBits, id);
        testType(columnName, PgType.BIT, -1, id);
        testType(columnName, PgType.BIT, 0, id);

        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            testType(columnName, PgType.BIT, random.nextInt(), id);
        }

        for (int i = 0; i < 10; i++) {
            BitSet bitSet = BitSet.valueOf(new long[]{0xFFFF_FFFFL & random.nextInt()});
            if (!bitSet.get(31)) {// bit type is fixed length.
                bitSet.set(31);
            }
            testType(columnName, PgType.BIT, bitSet, id);
        }

    }

    /**
     * @see PgType#VARBIT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-bit.html">Bit String Types</a>
     */
    final void doVarBitBindAndExtract() {
        final String columnName = "my_varbit_64";
        final long id = startId + 15;
        // bit type is fixed length.
        final char[] bitChars = new char[64];
        Arrays.fill(bitChars, '0');
        final String allZeroBits = new String(bitChars);
        Arrays.fill(bitChars, '1');
        final String allOneBits = new String(bitChars);


        testType(columnName, PgType.VARBIT, null, id);

        testType(columnName, PgType.VARBIT, allZeroBits, id);
        testType(columnName, PgType.VARBIT, allOneBits, id);
        testType(columnName, PgType.VARBIT, -1L, id);
        testType(columnName, PgType.VARBIT, 0L, id);

        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            testType(columnName, PgType.VARBIT, random.nextLong(), id);
            testType(columnName, PgType.BIT, random.nextInt(), id);
        }

        for (int i = 0; i < 10; i++) {
            BitSet bitSet = BitSet.valueOf(new long[]{random.nextLong()});
            testType(columnName, PgType.VARBIT, bitSet, id);
        }

    }

    /**
     * @see PgType#INTERVAL
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    final void doIntervalBindAndExtract() {
        final String columnName = "my_interval";
        final long id = startId + 16;

        testType(columnName, PgType.INTERVAL, null, id);

        testType(columnName, PgType.INTERVAL, Duration.ZERO, id);
        testType(columnName, PgType.INTERVAL, Period.ZERO, id);
        testType(columnName, PgType.INTERVAL, Interval.ZERO, id);
        testType(columnName, PgType.INTERVAL, Interval.of(Period.of(3, 8, 6), Duration.ofSeconds(3434, 999_999_999)), id);

        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_999_000), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_999_000).negated(), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_000_000), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_000_000).negated(), id);

        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_999_000).toString(), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_999_000).negated().toString(), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_000_000).toString(), id);
        testType(columnName, PgType.INTERVAL, Duration.ofSeconds(0, 999_000_000).negated().toString(), id);

        testType(columnName, PgType.INTERVAL, Duration.ZERO.toString(), id);
        testType(columnName, PgType.INTERVAL, Period.ZERO.toString(), id);
        testType(columnName, PgType.INTERVAL, Interval.ZERO.toString(), id);
        testType(columnName, PgType.INTERVAL, Duration.ofDays(2).plusMillis(99999).toString(), id);

        testType(columnName, PgType.INTERVAL, Interval.of(Period.of(3, 8, 6), Duration.ofSeconds(3434, 999_999_999)).toString(true), id);


        testType(columnName, PgType.INTERVAL, "P8MT98H23M22.333S", id);
        testType(columnName, PgType.INTERVAL, "P8MT98H23M-22.333S", id);
        testType(columnName, PgType.INTERVAL, "PT-0.999999S", id);
        testType(columnName, PgType.INTERVAL, "PT-0.9S", id);

        testType(columnName, PgType.INTERVAL, "PT0.9S", id);
        testType(columnName, PgType.INTERVAL, "-PT0.9S", id);
        testType(columnName, PgType.INTERVAL, "PT1M-60.9S", id);
        testType(columnName, PgType.INTERVAL, "PT-1M-60.9S", id);

        testType(columnName, PgType.INTERVAL, "PT1M60.9S", id);
        testType(columnName, PgType.INTERVAL, "PT2M-121.9S", id);
        testType(columnName, PgType.INTERVAL, "PT-2M121.9S", id);
        testType(columnName, PgType.INTERVAL, "PT2M-119.9S", id);

        testType(columnName, PgType.INTERVAL, "PT-2M119.9S", id);
        testType(columnName, PgType.INTERVAL, "PT2M-119.9S", id);
        testType(columnName, PgType.INTERVAL, "PT2M-0.9S", id);
        //TODO postgre modify 'P-1Y1M' to 'P-11M' , report this bug?
        // testType(columnName, PgType.INTERVAL, "P-1Y1M", id);

    }

    /**
     * @see PgType#BYTEA
     * @see <a href="https://www.postgresql.org/docs/current/datatype-binary.html">Binary data Types</a>
     */
    final void doByteaBindAndExtract() {
        final String columnName = "my_bytea";
        final long id = startId + 17;

        String text;
        byte[] array;

        testType(columnName, PgType.BYTEA, null, id);

        text = "Army's name,\\' \\ \" ; '' ";
        testType(columnName, PgType.BYTEA, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.BYTEA, array, id);

        text = ",SET balance = balance + 999.00 ";
        testType(columnName, PgType.BYTEA, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.BYTEA, array, id);

        text = "\\047 \\134 ";
        testType(columnName, PgType.BYTEA, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.BYTEA, array, id);

    }

    /**
     * @see PgType#MONEY
     * @see <a href="https://www.postgresql.org/docs/current/datatype-money.html">Monetary Types</a>
     */
    final void doMoneyBindAndExtract() {
        final String columnName = "my_money";
        final long id = startId + 18;

        testType(columnName, PgType.MONEY, null, id);

        testType(columnName, PgType.MONEY, "1000.00", id);
        testType(columnName, PgType.MONEY, BigDecimal.ZERO, id);
        testType(columnName, PgType.MONEY, new BigDecimal("1000.00"), id);
        testType(columnName, PgType.MONEY, 100L, id);

        testType(columnName, PgType.MONEY, 100, id);
        testType(columnName, PgType.MONEY, Short.MAX_VALUE, id);
        testType(columnName, PgType.MONEY, Byte.MAX_VALUE, id);

        testType(columnName, PgType.MONEY, BigInteger.ONE, id);


    }

    /**
     * @see PgType#VARCHAR
     * @see <a href="https://www.postgresql.org/docs/current/datatype-character.html"> Character Types</a>
     */
    final void doVarcharBindAndExtract() {
        final String columnName = "my_varchar";
        final long id = startId + 19;
        // TODO validate non-hex escapes

        String text;
        byte[] array;

        testType(columnName, PgType.VARCHAR, null, id);
        text = "Army's name,\\' \\ \" ; '' ";
        testType(columnName, PgType.VARCHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.VARCHAR, array, id);

        text = ",SET balance = balance + 999.00 ";
        testType(columnName, PgType.VARCHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.VARCHAR, array, id);

        text = "\\047 \\134 ";
        testType(columnName, PgType.VARCHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.VARCHAR, array, id);

    }

    /**
     * @see PgType#CHAR
     * @see <a href="https://www.postgresql.org/docs/current/datatype-character.html"> Character Types</a>
     */
    final void doCharBindAndExtract() {
        final String columnName = "my_char";
        final long id = startId + 20;
        // TODO validate non-hex escapes
        final int charMaxLength = 128;

        String text;
        byte[] array;
        int length;
        StringBuilder builder;

        testType(columnName, PgType.CHAR, null, id);

        text = "Army's name,\\' \\ \" ; '' ";
        builder = new StringBuilder(128);
        builder.append(text);
        length = charMaxLength - text.length();
        for (int i = 0; i < length; i++) {
            builder.append(' ');
        }
        text = builder.toString();

        testType(columnName, PgType.CHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.CHAR, array, id);

        text = ",SET balance = balance + 999.00 ";
        builder = new StringBuilder(128);
        builder.append(text);
        length = charMaxLength - text.length();
        for (int i = 0; i < length; i++) {
            builder.append(' ');
        }
        text = builder.toString();

        testType(columnName, PgType.CHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.CHAR, array, id);

        text = "\\047 \\134 ; '";
        builder = new StringBuilder(128);
        builder.append(text);
        length = charMaxLength - text.length();
        for (int i = 0; i < length; i++) {
            builder.append(' ');
        }
        text = builder.toString();

        testType(columnName, PgType.CHAR, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.CHAR, array, id);

    }

    /**
     * @see PgType#TEXT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-character.html"> Character Types</a>
     */
    final void doTextBindAndExtract() {
        final String columnName = "my_text";
        final long id = startId + 21;
        // TODO validate non-hex escapes
        String text;
        byte[] array;

        testType(columnName, PgType.TEXT, null, id);
        text = "Army's name,\\' \\ \" ; '' ";
        testType(columnName, PgType.TEXT, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.TEXT, array, id);

        text = ",SET balance = balance + 999.00 ";
        testType(columnName, PgType.TEXT, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.TEXT, array, id);

        text = "\\047 \\134 ";
        testType(columnName, PgType.TEXT, text, id);
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.TEXT, array, id);

    }

    /**
     * @see PgType#JSON
     * @see <a href="https://www.postgresql.org/docs/current/datatype-json.html"> JSON Types</a>
     */
    final void doJsonBindAndExtract() throws IOException {
        final String columnName = "my_json";
        final long id = startId + 22;
        // TODO validate non-hex escapes
        String json;
        byte[] array;
        Map<String, Object> map;

        final ObjectMapper mapper = new ObjectMapper();

        testType(columnName, PgType.JSON, null, id);

        map = Collections.emptyMap();
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSON, json, id);
        testType(columnName, PgType.JSON, array, id);

        json = mapper.writeValueAsString(new String[]{});
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSON, json, id);
        testType(columnName, PgType.JSON, array, id);


        map = new HashMap<>();
        map.put("id", 1L);
        map.put("name", "Army's name,\\' \\ \" ; '' ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSON, json, id);
        testType(columnName, PgType.JSON, array, id);


        map = new HashMap<>();
        map.put("id", 1L);
        map.put("name", ",SET balance = balance + 999.00 ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSON, json, id);
        testType(columnName, PgType.JSON, array, id);

        map = new HashMap<>();
        map.put("id", 1L);
        map.put("name", "\\047 \\134 ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSON, json, id);
        testType(columnName, PgType.JSON, array, id);

    }

    /**
     * @see PgType#JSONB
     * @see <a href="https://www.postgresql.org/docs/current/datatype-json.html"> JSON Types</a>
     */
    final void doJsonbBindAndExtract() throws IOException {
        final String columnName = "my_jsonb";
        final long id = startId + 23;
        // TODO validate non-hex escapes
        String json;
        byte[] array;
        Map<String, Object> map;

        final ObjectMapper mapper = new ObjectMapper();

        testType(columnName, PgType.JSONB, null, id);

        map = Collections.emptyMap();
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSONB, json, id);
        testType(columnName, PgType.JSONB, array, id);

        map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "Army's name,\\' \\ \" ; '' ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSONB, json, id);
        testType(columnName, PgType.JSONB, array, id);


        map = new HashMap<>();
        map.put("id", "1");
        map.put("name", ",SET balance = balance + 999.00 ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSONB, json, id);
        testType(columnName, PgType.JSONB, array, id);

        map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "\\047 \\134 ");
        json = mapper.writeValueAsString(map);
        array = json.getBytes(StandardCharsets.UTF_8);

        testType(columnName, PgType.JSONB, json, id);
        testType(columnName, PgType.JSONB, array, id);

    }

    /**
     * @see PgType#XML
     * @see <a href="https://www.postgresql.org/docs/current/datatype-xml.html">XML Types</a>
     */
    final void doXmlBindAndExtract() {
        final String columnName = "my_xml";
        final long id = startId + 24;
        String text;
        byte[] array;

        testType(columnName, PgType.XML, null, id);

        text = "<foo>''name''</foo><bar>\\slash</bar>";
        array = text.getBytes(StandardCharsets.UTF_8);
        testType(columnName, PgType.XML, text, id);
        testType(columnName, PgType.XML, array, id);

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/13/datatype-enum.html">Enumerated Types</a>
     */
    final void doEnumBindAndExtract() {
        final String columnName = "my_gender";
        final long id = startId + 25;

        testType(columnName, PgType.VARCHAR, null, id);

        testType(columnName, PgType.VARCHAR, Gender.FEMALE, id);
        testType(columnName, PgType.VARCHAR, Gender.MALE, id);
        testType(columnName, PgType.VARCHAR, Gender.UNKNOWN, id);

    }

    /**
     * @see PgType#UUID
     * @see <a href="https://www.postgresql.org/docs/current/datatype-uuid.html">UUID Types</a>
     */
    final void doUuidBindAndExtract() {
        final String columnName = "my_uuid";
        final long id = startId + 26;

        testType(columnName, PgType.UUID, null, id);

        testType(columnName, PgType.UUID, UUID.randomUUID(), id);
        testType(columnName, PgType.UUID, UUID.randomUUID().toString(), id);

    }

    /**
     * @see PgType#POINT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.5">Points</a>
     */
    final void doPointBindAndExtract() {
        final String columnName = "my_point";
        final long id = startId + 27;
        String text;

        testType(columnName, PgType.POINT, null, id);

        text = "(0,0)";
        testType(columnName, PgType.POINT, text, id);

        text = String.format("(%s,%s)", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.POINT, text, id);

    }

    /**
     * @see PgType#LINE
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LINE">Lines</a>
     */
    final void doLineBindAndExtract() {
        final String columnName = "my_line";
        final long id = startId + 28;
        String text;

        testType(columnName, PgType.LINE, null, id);

        text = "{1.0,-1.0,0.0}";
        testType(columnName, PgType.LINE, text, id);

        text = "{5.799010112459083E306,-1.0,-1.739703033737725E307}";
        testType(columnName, PgType.LINE, text, id);

    }

    /**
     * @see PgType#LINE_SEGMENT
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Line Segments</a>
     */
    final void doLineSegmentBindAndExtract() {
        final String columnName = "my_line_segment";
        final long id = startId + 29;
        String text;

        testType(columnName, PgType.LINE_SEGMENT, null, id);

        text = "[(0,0),(1,1)]";
        testType(columnName, PgType.LINE_SEGMENT, text, id);

        text = String.format("[(34.33,%s),(44.33,%s)]", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.LINE_SEGMENT, text, id);

    }

    /**
     * @see PgType#BOX
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.8"> Boxes</a>
     */
    final void doBoxBindAndExtract() {
        final String columnName = "my_box";
        final long id = startId + 30;
        String text;

        testType(columnName, PgType.BOX, null, id);

        text = "(0,0),(1,1)";
        testType(columnName, PgType.BOX, text, id);

        text = String.format("(3454.3,%s),(3456.334,%s)", Double.MIN_VALUE, Double.MAX_VALUE);
        testType(columnName, PgType.BOX, text, id);

    }

    /**
     * @see PgType#PATH
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9"> Paths</a>
     */
    final void doPathBindAndExtract() {
        final String columnName = "my_path";
        final long id = startId + 31;
        String text;

        testType(columnName, PgType.PATH, null, id);

        text = "[(0,0),(1,1),(2,2)]";
        testType(columnName, PgType.PATH, text, id);

        text = "((0,0),(1,1),(2,2))";
        testType(columnName, PgType.PATH, text, id);

        text = String.format("[(0,%s),(1,1),(2,%s)]", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.PATH, text, id);

        text = String.format("((0,%s),(1,1),(2,%s))", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.PATH, text, id);


    }

    /**
     * @see PgType#POLYGON
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-POLYGON"> Polygons</a>
     */
    final void doPolygonBindAndExtract() {
        final String columnName = "my_polygon";
        final long id = startId + 32;
        String text;

        testType(columnName, PgType.POLYGON, null, id);

        text = "((0,0),(1,1),(2,2))";
        testType(columnName, PgType.POLYGON, text, id);

        text = String.format("((0,%s),(1,1),(2,%s))", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.POLYGON, text, id);

    }

    /**
     * @see PgType#CIRCLES
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-CIRCLE"> Circles</a>
     */
    final void doCircleBindAndExtract() {
        final String columnName = "my_circles";
        final long id = startId + 33;
        String text;

        testType(columnName, PgType.CIRCLES, null, id);

        text = "<(0,0),5.3>";
        testType(columnName, PgType.CIRCLES, text, id);

        text = String.format("<(%s,%s),8.8>", Double.MIN_VALUE, Double.MAX_VALUE);
        testType(columnName, PgType.CIRCLES, text, id);

    }

    /**
     * @see PgType#CIDR
     * @see <a href="https://www.postgresql.org/docs/current/datatype-net-types.html"> Network Address Types</a>
     */
    final void doCidrBindAndExtract() {
        final String columnName = "my_cidr";
        final long id = startId + 34;
        String text;

        testType(columnName, PgType.CIDR, null, id);

        text = "192.168.0.0/24";

        testType(columnName, PgType.CIDR, text, id);
    }

    /**
     * @see PgType#INET
     * @see <a href="https://www.postgresql.org/docs/current/datatype-net-types.html"> Network Address Types</a>
     */
    final void doInetBindAndExtract() {
        final String columnName = "my_inet";
        final long id = startId + 35;
        String text;

        testType(columnName, PgType.INET, null, id);

        text = "192.168.0.0/24";
        testType(columnName, PgType.INET, text, id);

        text = "192.168.0.1/24";
        testType(columnName, PgType.INET, text, id);
    }

    /**
     * @see PgType#MACADDR
     * @see <a href="https://www.postgresql.org/docs/current/datatype-net-types.html"> Network Address Types</a>
     */
    final void doMacaddrBindAndExtract() {
        final String columnName = "my_macaddr";
        final long id = startId + 36;
        String text;

        testType(columnName, PgType.MACADDR, null, id);

        text = "08:00:2b:01:02:03";
        testType(columnName, PgType.MACADDR, text, id);

    }


    /**
     * @see PgType#MACADDR8
     * @see <a href="https://www.postgresql.org/docs/current/datatype-net-types.html"> Network Address Types</a>
     */
    final void doMacaddr8BindAndExtract() {
        final String columnName = "my_macaddr8";
        final long id = startId + 37;
        String text;

        testType(columnName, PgType.MACADDR8, null, id);

        text = "08:00:2b:01:02:03:04:05";
        testType(columnName, PgType.MACADDR8, text, id);

    }

    /**
     * @see PgType#TSVECTOR
     * @see <a href="https://www.postgresql.org/docs/current/datatype-textsearch.html">Text Search Types</a>
     */
    final void doTsvectorBindAndExtract() {
        final String columnName = "my_tsvector";
        final long id = startId + 38;
        String text;
        ResultRow row;
        List<String> lexemeList;

        testType(columnName, PgType.TSVECTOR, null, id);

        text = "Army's name 'this is, entirety' ";
        row = testType(columnName, PgType.TSVECTOR, text, id);
        lexemeList = row.getList(columnName, String.class);
        assertEquals(lexemeList.size(), 3, columnName);

        text = ",SET balance = balance + 999.00 ";
        row = testType(columnName, PgType.TSVECTOR, text, id);
        lexemeList = row.getList(columnName, String.class);
        assertEquals(lexemeList.size(), 5, columnName);


        text = "\\047 \\134 ";
        row = testType(columnName, PgType.TSVECTOR, text, id);
        lexemeList = row.getList(columnName, String.class);
        assertEquals(lexemeList.size(), 2, columnName);


    }

    /**
     * @see PgType#INT4RANGE
     * @see PgType#INT8RANGE
     * @see PgType#NUMRANGE
     * @see PgType#TSRANGE
     * @see PgType#TSTZRANGE
     * @see PgType#DATERANGE
     * @see <a href="https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-BUILTIN">Range Types</a>
     */
    final void doRangeBindAndExtract() {
        // only test int4range,because binding and extract is same.
        final String columnName = "my_int4_range";
        final long id = startId + 39;
        String text, result;
        ResultRow row;

        testType(columnName, PgType.INT4RANGE, null, id);

        text = "[1,9]";
        row = testType(columnName, PgType.INT4RANGE, text, id);
        result = row.getNonNull(columnName, String.class);
        if (!result.equals(text)
                && !result.equals("[1,10)")
                && !result.equals("(0,10)")
                && !result.equals("(0,9]")) {
            fail(columnName);
        }


        text = "empty";
        row = testType(columnName, PgType.INT4RANGE, text, id);
        assertEquals(row.get(columnName, String.class), text, text);
    }

    /**
     * @see PgType#SMALLINT_ARRAY
     */
    final void doSmallintArrayBindAndExtract() {
        final String columnName = "my_smallint_array";
        final long id = startId + 40;
        Object array;

        testType(columnName, PgType.SMALLINT_ARRAY, null, id);

        array = new Short[]{null};
        testType(columnName, PgType.SMALLINT_ARRAY, array, id);
    }


    private ResultRow testType(final String columnName, final PgType columnType
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
        return row;
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
        final PgType sqlType = (PgType) row.getRowMeta().getSQLType(columnName);
        switch (sqlType) {
            case SMALLINT:
            case INTEGER:
            case BIGINT: {
                final Long v;
                if (nonNull instanceof String) {
                    v = Long.parseLong((String) nonNull);
                } else if (nonNull instanceof Boolean) {
                    v = (Boolean) nonNull ? 1L : 0L;
                } else {
                    v = ((Number) nonNull).longValue();
                }
                assertEquals(row.get(columnName, Long.class), v, columnName);
            }
            break;
            case DECIMAL: {
                final BigDecimal v;
                if (nonNull instanceof String) {
                    v = new BigDecimal((String) nonNull);
                } else if (nonNull instanceof BigDecimal) {
                    v = (BigDecimal) nonNull;
                } else if (nonNull instanceof BigInteger) {
                    v = new BigDecimal((BigInteger) nonNull);
                } else if (nonNull instanceof Boolean) {
                    v = (Boolean) nonNull ? BigDecimal.ONE : BigDecimal.ZERO;
                } else {
                    v = BigDecimal.valueOf(((Number) nonNull).longValue());
                }
                final BigDecimal result = row.getNonNull(columnName, BigDecimal.class);
                assertEquals(result.compareTo(v), 0, columnName);
            }
            break;
            case DOUBLE: {
                final Double v;
                if (nonNull instanceof String) {
                    v = Double.parseDouble((String) nonNull);
                } else if (nonNull instanceof Boolean) {
                    v = (Boolean) nonNull ? 1.0D : 0.0D;
                } else {
                    v = ((Number) nonNull).doubleValue();
                }
                assertEquals(row.get(columnName, Double.class), v, columnName);
            }
            break;
            case REAL: {
                final Float v;
                if (nonNull instanceof String) {
                    v = Float.parseFloat((String) nonNull);
                } else if (nonNull instanceof Boolean) {
                    v = (Boolean) nonNull ? 1.0F : 0.0F;
                } else {
                    v = ((Number) nonNull).floatValue();
                }
                assertEquals(row.get(columnName, Float.class), v, columnName);
            }
            break;
            case BOOLEAN: {
                final boolean bindValue;
                if (nonNull instanceof String) {
                    final String v = (String) nonNull;
                    if (v.equalsIgnoreCase(PgConstant.TRUE)
                            || v.equalsIgnoreCase("T")) {
                        bindValue = true;
                    } else if (v.equalsIgnoreCase(PgConstant.FALSE)
                            || v.equalsIgnoreCase("F")) {
                        bindValue = false;
                    } else {
                        throw new IllegalArgumentException(String.format("value[%s] error.", nonNull));
                    }
                } else if (nonNull instanceof Boolean) {
                    bindValue = (Boolean) nonNull;
                } else {
                    bindValue = ((Number) nonNull).longValue() == 1L;
                }
                assertEquals(row.get(columnName, Boolean.class), Boolean.valueOf(bindValue), columnName);
            }
            break;
            case DATE: {
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
            }
            break;
            case TIME: {
                final LocalTime v;
                if (nonNull instanceof String) {
                    v = LocalTime.parse((String) nonNull, PgTimes.ISO_LOCAL_TIME_FORMATTER);
                } else {
                    v = (LocalTime) nonNull;
                }
                // database possibly round
                final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, LocalTime.class));
                final int count = 6 - row.getRowMeta().getScale(columnName);
                int multi = 1;
                for (int i = 0; i < count; i++) {
                    multi *= 10;
                }
                assertTrue(intervalMicro >= 0 && intervalMicro <= multi, columnName);
            }
            break;
            case TIMETZ: {
                final OffsetTime v;
                if (nonNull instanceof String) {
                    v = OffsetTime.parse((String) nonNull, PgTimes.ISO_OFFSET_TIME_FORMATTER);
                } else {
                    v = (OffsetTime) nonNull;
                }
                // database possibly round
                final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, OffsetTime.class));
                final int count = 6 - row.getRowMeta().getScale(columnName);
                int multi = 1;
                for (int i = 0; i < count; i++) {
                    multi *= 10;
                }
                assertTrue(intervalMicro >= 0 && intervalMicro <= multi, columnName);
            }
            break;
            case TIMESTAMP: {
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
                final int count = 6 - row.getRowMeta().getScale(columnName);
                int multi = 1;
                for (int i = 0; i < count; i++) {
                    multi *= 10;
                }
                assertTrue(intervalMicro >= 0 && intervalMicro <= multi, columnName);
            }
            break;
            case TIMESTAMPTZ: {
                final OffsetDateTime v;
                if (nonNull instanceof String) {
                    if (assertDateOrTimestampSpecial(columnName, row, nonNull)) {
                        return;
                    }
                    v = OffsetDateTime.parse((String) nonNull, PgTimes.ISO_OFFSET_DATETIME_FORMATTER);
                } else if (nonNull instanceof ZonedDateTime) {
                    v = ((ZonedDateTime) nonNull).toOffsetDateTime();
                } else {
                    v = (OffsetDateTime) nonNull;
                }
                // database possibly round
                final long intervalMicro = ChronoUnit.MICROS.between(v, row.getNonNull(columnName, OffsetDateTime.class));
                final int count = 6 - row.getRowMeta().getScale(columnName);
                int multi = 1;
                for (int i = 0; i < count; i++) {
                    multi *= 10;
                }
                assertTrue(intervalMicro >= 0 && intervalMicro <= multi, columnName);
            }
            break;
            case BIT:
            case VARBIT: {
                final BitSet v;
                if (nonNull instanceof String) {
                    v = PgStrings.bitStringToBitSet((String) nonNull, false);
                } else if (nonNull instanceof Long) {
                    v = BitSet.valueOf(new long[]{(Long) nonNull});
                } else if (nonNull instanceof Integer) {
                    final int i = (Integer) nonNull;
                    v = BitSet.valueOf(new long[]{0xFFFF_FFFFL & i});
                } else {
                    v = (BitSet) nonNull;
                }
                assertEquals(row.get(columnName, BitSet.class), v, columnName);
            }
            break;
            case INTERVAL: {
                final Interval v;
                if (nonNull instanceof String) {
                    v = Interval.parse((String) nonNull);
                } else if (nonNull instanceof Duration) {
                    v = Interval.of((Duration) nonNull);
                } else if (nonNull instanceof Period) {
                    v = Interval.of((Period) nonNull);
                } else {
                    v = (Interval) nonNull;
                }
                final Interval r = row.getNonNull(columnName, Interval.class);
                boolean equal = r.equals(v, true);
                assertTrue(equal, String.format("column[%s] result[%s] binding[%s]", columnName, r, v));
            }
            break;
            case MONEY: {
                final BigDecimal v;
                if (nonNull instanceof String) {
                    v = new BigDecimal((String) nonNull);
                } else if (nonNull instanceof BigInteger) {
                    v = new BigDecimal((BigInteger) nonNull);
                } else {
                    v = BigDecimal.valueOf(((Number) nonNull).longValue());
                }
                assertEquals(row.getNonNull(columnName, BigDecimal.class).compareTo(v), 0, columnName);
            }
            break;
            case JSONB: {
                assertJsonbResult(columnName, row, nonNull);
            }
            break;
            case BOX: {
                final PgBox v = PgGeometries.box(nonNull.toString());
                assertEquals(row.get(columnName, PgBox.class), v, columnName);
            }
            break;
            case POINT: {
                final Point v = PgGeometries.point(nonNull.toString());
                assertEquals(row.get(columnName, Point.class), v, columnName);
            }
            break;
            case LINE: {
                final PgLine v = PgGeometries.line(nonNull.toString());
                assertEquals(row.get(columnName, PgLine.class), v, columnName);
            }
            break;
            case LINE_SEGMENT: {
                final Line v = PgGeometries.lineSegment(nonNull.toString());
                assertEquals(row.get(columnName, Line.class), v, columnName);
            }
            break;
            case PATH: {
                final LineString v = PgGeometries.path(nonNull.toString());
                assertEquals(row.get(columnName, LineString.class), v, columnName);
            }
            break;
            case POLYGON: {
                final PgPolygon v = PgGeometries.polygon(nonNull.toString());
                assertEquals(row.get(columnName, PgPolygon.class), v, columnName);
            }
            break;
            case CIRCLES: {
                final Circle v = PgGeometries.circle(nonNull.toString());
                assertEquals(row.get(columnName, Circle.class), v, columnName);
            }
            break;
            case TSVECTOR: {
                // TSVECTOR type can't compare ,only test whether  can convert to lexemes list or not.
                // And binding same with varchar
                final List<String> lexemeList = row.getList(columnName, String.class);
                log.info("TSVECTOR type result : {}", lexemeList);
                assertFalse(lexemeList.isEmpty(), columnName);
            }
            break;
            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case TSRANGE:
            case TSTZRANGE:
            case DATERANGE:
                //no-op
                break;
            default: {
                assertEquals(row.get(columnName, nonNull.getClass()), nonNull, columnName);
            }

        }


    }

    @SuppressWarnings("unchecked")
    private void assertJsonbResult(final String columnName, final ResultRow row, final Object nonNull) {
        final Map<String, Object> v;
        final Map<String, Object> result;
        try {
            final ObjectMapper mapper = new ObjectMapper();
            if (nonNull instanceof String) {
                v = (Map<String, Object>) mapper.readValue((String) nonNull, Map.class);
            } else {

                byte[] bytes = (byte[]) nonNull;
                v = (Map<String, Object>) mapper.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
            }

            result = (Map<String, Object>) mapper.readValue(row.get(columnName, String.class), Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals(result, v, columnName);
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
