package io.jdbd.postgre.protocol.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.type.*;
import io.jdbd.postgre.util.*;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.type.Interval;
import io.jdbd.type.Point;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LongString;
import io.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiPredicate;

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
     * @see PgType#FLOAT8
     */
    final void doDoubleBindAndExtract() {
        final String columnName = "my_double";
        final long id = startId + 6;

        testType(columnName, PgType.FLOAT8, null, id);

        testType(columnName, PgType.FLOAT8, Double.MAX_VALUE, id);
        testType(columnName, PgType.FLOAT8, Double.MIN_VALUE, id);
        testType(columnName, PgType.FLOAT8, Double.toString(Double.MAX_VALUE), id);
        testType(columnName, PgType.FLOAT8, Double.toString(Double.MAX_VALUE), id);

        testType(columnName, PgType.FLOAT8, 0.0D, id);
        testType(columnName, PgType.FLOAT8, Boolean.TRUE, id);
        testType(columnName, PgType.FLOAT8, Boolean.FALSE, id);

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
        ResultRow row;

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
        row = testType(columnName, PgType.TIMESTAMP, "0000-01-01 23:59:59", id);
        row.getNonNull(columnName, String.class);

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
     * @see PgType#LSEG
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Line Segments</a>
     */
    final void doLineSegmentBindAndExtract() {
        final String columnName = "my_line_segment";
        final long id = startId + 29;
        String text;

        testType(columnName, PgType.LSEG, null, id);

        text = "[(0,0),(1,1)]";
        testType(columnName, PgType.LSEG, text, id);

        text = String.format("[(34.33,%s),(44.33,%s)]", Double.MAX_VALUE, Double.MIN_VALUE);
        testType(columnName, PgType.LSEG, text, id);

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
     * @see PgType#CIRCLE
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-CIRCLE"> Circles</a>
     */
    final void doCircleBindAndExtract() {
        final String columnName = "my_circles";
        final long id = startId + 33;
        String text;

        testType(columnName, PgType.CIRCLE, null, id);

        text = "<(0,0),5.3>";
        testType(columnName, PgType.CIRCLE, text, id);

        text = String.format("<(%s,%s),8.8>", Double.MIN_VALUE, Double.MAX_VALUE);
        testType(columnName, PgType.CIRCLE, text, id);

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
        String columnName;
        final long id = startId + 40;
        Object array;
        ResultRow row;

        columnName = "my_smallint_array";
        testType(columnName, PgType.SMALLINT_ARRAY, null, id);

        array = new Short[]{null};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, Short[][].class);
        row.getNonNull(columnName, Short[][][].class);

        array = new short[]{234, Short.MAX_VALUE, Short.MIN_VALUE, 0};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, short[].class);


        array = new Short[]{23, 34, -1, 0, Short.MIN_VALUE, Short.MAX_VALUE, null};
        testType(columnName, PgType.SMALLINT_ARRAY, array, id);

        // my_smallint_array_2

        columnName = "my_smallint_array_2";
        testType(columnName, PgType.SMALLINT_ARRAY, null, id);

        array = new Short[][]{null};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, Short[][].class);
        row.getNonNull(columnName, Short[][][].class);
        row.getNonNull(columnName, Short[][][][].class);

        array = new short[][]{{Short.MAX_VALUE, Short.MIN_VALUE,}, {0, -1}};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, short[][].class);


        array = new Short[][]{{Short.MAX_VALUE, Short.MIN_VALUE,}, {0, -1}, {null, null}};
        testType(columnName, PgType.SMALLINT_ARRAY, array, id);

        // my_smallint_array_4

        columnName = "my_smallint_array_4";
        testType(columnName, PgType.SMALLINT_ARRAY, null, id);

        array = new Short[][][][]{null};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, Short[][][][].class);
        row.getNonNull(columnName, Short[][][][][].class);

        array = new short[][][][]{{{{Short.MAX_VALUE, Short.MIN_VALUE,}, {0, -1}}}};
        row = testType(columnName, PgType.SMALLINT_ARRAY, array, id);
        row.getNonNull(columnName, short[][][][].class);


        array = new Short[][][][]{{{{Short.MAX_VALUE, Short.MIN_VALUE,}, {0, -1}, {null, null}}}};
        testType(columnName, PgType.SMALLINT_ARRAY, array, id);

    }

    /**
     * @see PgType#INTEGER_ARRAY
     */
    final void doIntegerArrayBindAndExtract() {
        String columnName;
        final long id = startId + 41;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_integer_array";
        testType(columnName, PgType.INTEGER_ARRAY, null, id);

        array = new Integer[]{null};
        row = testType(columnName, PgType.INTEGER_ARRAY, array, id);
        row.getNonNull(columnName, Integer[].class);
        row.getNonNull(columnName, Integer[][][][].class);
        row.getNonNull(columnName, Integer[][][][][].class);

        array = new int[]{234, Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        row = testType(columnName, PgType.INTEGER_ARRAY, array, id);
        row.getNonNull(columnName, Integer[].class);


        array = new Integer[]{234, Integer.MAX_VALUE, Integer.MIN_VALUE, 0, null};
        testType(columnName, PgType.INTEGER_ARRAY, array, id);

        // below two dimension array
        columnName = "my_integer_array_2";
        testType(columnName, PgType.INTEGER_ARRAY, null, id);

        array = new Integer[][]{null};
        row = testType(columnName, PgType.INTEGER_ARRAY, array, id);
        row.getNonNull(columnName, Integer[].class);
        row.getNonNull(columnName, Integer[][].class);
        row.getNonNull(columnName, Integer[][][][].class);

        array = new int[][]{{234, Integer.MAX_VALUE, Integer.MIN_VALUE, 0}};
        row = testType(columnName, PgType.INTEGER_ARRAY, array, id);
        row.getNonNull(columnName, Integer[][].class);


        array = new Integer[][]{{234, Integer.MAX_VALUE, Integer.MIN_VALUE, 0, null}};
        testType(columnName, PgType.INTEGER_ARRAY, array, id);

    }

    /**
     * @see PgType#BIGINT_ARRAY
     */
    final void doBigIntArrayBindAndExtract() {
        String columnName;
        final long id = startId + 42;
        final PgType pgType = PgType.BIGINT_ARRAY;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_bigint_array";
        testType(columnName, pgType, null, id);

        array = new Long[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][][][].class);
        row.getNonNull(columnName, Long[][][][][].class);

        array = new long[]{234L, Long.MAX_VALUE, Long.MIN_VALUE, 0};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);


        array = new Long[]{234L, Long.MAX_VALUE, Long.MIN_VALUE, 0L, null};
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_bigint_array_2";
        testType(columnName, pgType, null, id);

        array = new Long[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][].class);
        row.getNonNull(columnName, Long[][][][].class);

        array = new long[][]{{234L, Long.MAX_VALUE, Long.MIN_VALUE, 0L}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[][].class);


        array = new Long[][]{{234L, Long.MAX_VALUE, Long.MIN_VALUE, 0L, null}};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#DECIMAL_ARRAY
     */
    final void doDecimalArrayBindAndExtract() {
        String columnName;
        final long id = startId + 43;
        final PgType pgType = PgType.DECIMAL_ARRAY;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_decimal_array";
        testType(columnName, pgType, null, id);

        array = new BigDecimal[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][][][].class);

        array = new Object[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][][][].class);

        array = new BigDecimal[]{new BigDecimal("-0.33"), BigDecimal.ZERO, BigDecimal.ONE, new BigDecimal("342343234.33")};
        testType(columnName, pgType, array, id);

        array = new Object[]{"NaN", BigDecimal.ZERO, BigDecimal.ONE, "NaN"};
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_decimal_array_2";
        testType(columnName, pgType, null, id);

        array = new BigDecimal[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][].class);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, BigDecimal[][][][].class);

        array = new Object[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][].class);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, BigDecimal[][][][].class);
        ;

        array = new BigDecimal[]{new BigDecimal("-0.33"), BigDecimal.ZERO, BigDecimal.ONE, new BigDecimal("342343234.33")};
        testType(columnName, pgType, array, id);

        array = new Object[]{"NaN", BigDecimal.ZERO, BigDecimal.ONE, "NaN"};
        testType(columnName, pgType, array, id);

    }

    /**
     * @see PgType#REAL_ARRAY
     */
    final void doRealArrayBindAndExtract() {
        String columnName;
        final long id = startId + 44;
        final PgType pgType = PgType.REAL_ARRAY;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_real_array";
        testType(columnName, pgType, null, id);

        array = new Float[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Float[].class);
        row.getNonNull(columnName, Float[][].class);
        row.getNonNull(columnName, Float[][][][].class);

        array = new float[]{-1.0F, 0.0F, Float.MAX_VALUE, Float.MIN_VALUE};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Float[].class);

        array = new Float[]{-1.0F, 0.0F, Float.MAX_VALUE, Float.MIN_VALUE, null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Float[].class);

        // below two dimension array

        columnName = "my_real_array_2";
        testType(columnName, pgType, null, id);

        array = new Float[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Float[].class);
        row.getNonNull(columnName, Float[][].class);
        row.getNonNull(columnName, Float[][][][].class);

        array = new float[][]{{-1.0F, 0.0F, Float.MAX_VALUE, Float.MIN_VALUE}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Float[][].class);

        array = new Float[][]{{-1.0F, 0.0F, Float.MAX_VALUE, Float.MIN_VALUE, null}};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#FLOAT8_ARRAY
     */
    final void doDoubleArrayBindAndExtract() {
        String columnName;
        final long id = startId + 45;
        final PgType pgType = PgType.FLOAT8_ARRAY;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_double_array";
        testType(columnName, pgType, null, id);

        array = new Double[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Double[].class);
        row.getNonNull(columnName, Double[][].class);
        row.getNonNull(columnName, Double[][][][].class);

        array = new double[]{-1.0D, 0.0D, Double.MAX_VALUE, Double.MIN_VALUE};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Double[].class);

        array = new Double[]{-1.0D, 0.0D, Double.MAX_VALUE, Double.MIN_VALUE, null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Double[].class);

        // below two dimension array

        columnName = "my_double_array_2";
        testType(columnName, pgType, null, id);

        array = new Double[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Double[].class);
        row.getNonNull(columnName, Double[][].class);
        row.getNonNull(columnName, Double[][][][].class);

        array = new double[][]{{-1.0D, 0.0D, Double.MAX_VALUE, Double.MIN_VALUE}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Double[][].class);

        array = new Double[][]{{-1.0D, 0.0D, Double.MAX_VALUE, Double.MIN_VALUE, null}};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#BOOLEAN_ARRAY
     */
    final void doBooleanArrayBindAndExtract() {
        String columnName;
        final long id = startId + 46;
        final PgType pgType = PgType.BOOLEAN_ARRAY;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_boolean_array";
        testType(columnName, pgType, null, id);

        array = new Boolean[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Boolean[].class);
        row.getNonNull(columnName, Boolean[][].class);

        array = new boolean[]{true, false};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Boolean[].class);

        array = new Boolean[]{true, false, null};
        testType(columnName, pgType, array, id);

        // below two dimension array

        columnName = "my_boolean_array_2";
        testType(columnName, pgType, null, id);

        array = new Boolean[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Boolean[].class);
        row.getNonNull(columnName, Boolean[][].class);

        array = new boolean[][]{{true, false}, {false, true}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Boolean[][].class);

        array = new Boolean[][]{{true, false, null}, {Boolean.FALSE, Boolean.TRUE, null}};
        testType(columnName, pgType, array, id);

    }

    /**
     * @see PgType#TIMESTAMP_ARRAY
     */
    final void doTimestampArrayBindAndExtract() {
        String columnName;
        final long id = startId + 47;
        final PgType pgType = PgType.TIMESTAMP_ARRAY;
        final DateTimeFormatter pgIso = PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_timestamp_array";
        testType(columnName, pgType, null, id);

        array = new LocalDateTime[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LocalDateTime[].class);
        row.getNonNull(columnName, LocalDateTime[][].class);

        array = new LocalDateTime[]{
                LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso),
                LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso),
                LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso),
                LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso),
                null};

        testType(columnName, pgType, array, id);

        array = new Object[]{
                LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso),
                LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso),
                LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso),
                LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso),
                null,
                PgConstant.INFINITY,
                PgConstant.NEG_INFINITY
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, String[].class);


        // below two dimension array
        columnName = "my_timestamp_array_2";
        testType(columnName, pgType, null, id);

        array = new LocalDateTime[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, LocalDateTime[].class);
        row.getNonNull(columnName, LocalDateTime[][].class);

        array = new LocalDateTime[][]{
                {LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso),
                        LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso),
                        LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso),
                        LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso),
                        null}
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, String[][].class);

        array = new Object[][]{
                {LocalDateTime.parse("4713-01-01 00:00:00 BC", pgIso),
                        LocalDateTime.parse("4713-01-01 23:59:59 BC", pgIso),
                        LocalDateTime.parse("4713-12-31 00:00:00 BC", pgIso),
                        LocalDateTime.parse("4713-12-31 23:59:59 BC", pgIso),
                        null,
                        PgConstant.INFINITY,
                        PgConstant.NEG_INFINITY
                }
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, String[][].class);
    }

    /**
     * @see PgType#TIME_ARRAY
     */
    final void doTimeArrayBindAndExtract() {
        String columnName;
        final long id = startId + 48;
        final PgType pgType = PgType.TIME_ARRAY;

        Object array;

        // below one dimension array
        columnName = "my_time_array";
        testType(columnName, pgType, null, id);

        array = new LocalTime[]{LocalTime.parse("23:59:59"), LocalTime.MIDNIGHT, LocalTime.NOON, null};
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_time_array_2";
        testType(columnName, pgType, null, id);

        array = new LocalTime[][]{
                {LocalTime.parse("23:59:59"), LocalTime.MIDNIGHT, LocalTime.NOON, null}
        };
        testType(columnName, pgType, array, id);

    }


    /**
     * @see PgType#TIMESTAMPTZ_ARRAY
     */
    final void doTimestampTzArrayBindAndExtract() {
        String columnName;
        final long id = startId + 49;
        final PgType pgType = PgType.TIMESTAMPTZ_ARRAY;
        final DateTimeFormatter pgIso = PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_zoned_timestamp_array";
        testType(columnName, pgType, null, id);

        array = new OffsetDateTime[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, OffsetDateTime[].class);
        row.getNonNull(columnName, OffsetDateTime[][].class);

        array = new OffsetDateTime[]{
                OffsetDateTime.parse("4713-01-01 00:00:00+00:00 BC", pgIso),
                OffsetDateTime.parse("4713-01-01 23:59:59+00:00 BC", pgIso),
                OffsetDateTime.parse("4713-12-31 00:00:00+00:00 BC", pgIso),
                OffsetDateTime.parse("4713-12-31 23:59:59+00:00 BC", pgIso),
                null};

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, ZonedDateTime[].class);

        array = new ZonedDateTime[]{
                ZonedDateTime.parse("4713-01-01 00:00:00+00:00 BC", pgIso),
                ZonedDateTime.parse("4713-01-01 23:59:59+00:00 BC", pgIso),
                ZonedDateTime.parse("4713-12-31 00:00:00+00:00 BC", pgIso),
                ZonedDateTime.parse("4713-12-31 23:59:59+00:00 BC", pgIso),
                null};

        testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, ZonedDateTime[].class);

        array = new Object[]{
                OffsetDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso),
                OffsetDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso),
                OffsetDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso),
                OffsetDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso),
                null,
                ZonedDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso),
                ZonedDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso),
                ZonedDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso),
                ZonedDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso),
                null,
                PgConstant.INFINITY,
                PgConstant.NEG_INFINITY
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, String[].class);

        // below two dimension array
        columnName = "my_zoned_timestamp_array_2";
        testType(columnName, pgType, null, id);

        array = new OffsetDateTime[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, OffsetDateTime[].class);
        row.getNonNull(columnName, OffsetDateTime[][].class);

        array = new OffsetDateTime[][]{
                {
                        OffsetDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso),
                        null}
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, ZonedDateTime[][].class);

        array = new Object[][]{
                {
                        OffsetDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso),
                        OffsetDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso),
                        null,
                        ZonedDateTime.parse("4713-01-01 00:00:00+08:00 BC", pgIso),
                        ZonedDateTime.parse("4713-01-01 23:59:59+08:00 BC", pgIso),
                        ZonedDateTime.parse("4713-12-31 00:00:00+08:00 BC", pgIso),
                        ZonedDateTime.parse("4713-12-31 23:59:59+08:00 BC", pgIso),
                        null,
                        PgConstant.INFINITY,
                        PgConstant.NEG_INFINITY
                }
        };

        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, String[][].class);


    }

    /**
     * @see PgType#TIMETZ_ARRAY
     */
    final void doTimeTzArrayBindAndExtract() {
        String columnName;
        final long id = startId + 50;
        final PgType pgType = PgType.TIMETZ_ARRAY;
        final ZoneOffset offset = PgTimes.systemZoneOffset();

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_zoned_time_array";
        testType(columnName, pgType, null, id);

        array = new OffsetTime[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, OffsetTime[].class);
        row.getNonNull(columnName, OffsetTime[][].class);

        array = new OffsetTime[]{
                OffsetTime.of(LocalTime.MIN, offset),
                OffsetTime.of(LocalTime.MIDNIGHT, offset),
                OffsetTime.of(LocalTime.NOON, offset),
                OffsetTime.of(LocalTime.parse("23:59:59"), offset),
                null
        };
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_zoned_time_array_2";
        testType(columnName, pgType, null, id);

        array = new OffsetTime[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, OffsetTime[].class);
        row.getNonNull(columnName, OffsetTime[][].class);

        array = new OffsetTime[][]{
                {
                        OffsetTime.of(LocalTime.MIN, offset),
                        OffsetTime.of(LocalTime.MIDNIGHT, offset),
                        OffsetTime.of(LocalTime.NOON, offset),
                        OffsetTime.of(LocalTime.parse("23:59:59"), offset),
                        null
                }
        };
        testType(columnName, pgType, array, id);

    }

    /**
     * @see PgType#DATE_ARRAY
     */
    final void doDateArrayBindAndExtract() {
        String columnName;
        final long id = startId + 51;
        final PgType pgType = PgType.DATE_ARRAY;
        final DateTimeFormatter pgIso = PgTimes.PG_ISO_LOCAL_DATE_FORMATTER;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_date_array";
        testType(columnName, pgType, null, id);

        array = new LocalDate[]{
                LocalDate.parse("4713-01-01 BC", pgIso),
                LocalDate.parse("4713-01-01 BC", pgIso),
                LocalDate.parse("4713-12-31 BC", pgIso),
                LocalDate.parse("4713-12-31 BC", pgIso),
                null
        };
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[].class);
        row.getNonNull(columnName, String[].class);

        array = new Object[]{
                LocalDate.parse("4713-01-01 BC", pgIso),
                LocalDate.parse("4713-01-01 BC", pgIso),
                LocalDate.parse("4713-12-31 BC", pgIso),
                LocalDate.parse("4713-12-31 BC", pgIso),
                null,
                PgConstant.INFINITY,
                PgConstant.NEG_INFINITY
        };
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        // below one dimension array
        columnName = "my_date_array_2";
        testType(columnName, pgType, null, id);

        array = new LocalDate[][]{
                {
                        LocalDate.parse("4713-01-01 BC", pgIso),
                        LocalDate.parse("4713-01-01 BC", pgIso),
                        LocalDate.parse("4713-12-31 BC", pgIso),
                        LocalDate.parse("4713-12-31 BC", pgIso),
                        null
                }
        };
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Object[][].class);
        row.getNonNull(columnName, String[][].class);

        array = new Object[][]{
                {
                        LocalDate.parse("4713-01-01 BC", pgIso),
                        LocalDate.parse("4713-01-01 BC", pgIso),
                        LocalDate.parse("4713-12-31 BC", pgIso),
                        LocalDate.parse("4713-12-31 BC", pgIso),
                        null,
                        PgConstant.INFINITY,
                        PgConstant.NEG_INFINITY
                }
        };
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
    }

    /**
     * @see PgType#BIT_ARRAY
     */
    final void doBitArrayBindAndExtract() {
        String columnName;
        final long id = startId + 52;
        final PgType pgType = PgType.BIT_ARRAY;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_bit64_array";
        testType(columnName, pgType, null, id);

        array = new Long[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][].class);


        array = new long[]{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);

        array = new Long[]{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, null};
        testType(columnName, pgType, array, id);

        array = new String[]{PgStrings.toBinaryString(0L, false), PgStrings.toBinaryString(-1L, false), PgStrings.toBinaryString(Long.MIN_VALUE, false), PgStrings.toBinaryString(Long.MAX_VALUE, false), null};
        testType(columnName, pgType, array, id);

        array = new BitSet[]{BitSet.valueOf(new long[]{-1L})};
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_bit64_array_2";
        testType(columnName, pgType, null, id);

        array = new Long[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][].class);

        array = new long[][]{{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[][].class);

        array = new Long[][]{{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, null}};
        testType(columnName, pgType, array, id);

        array = new String[][]{{PgStrings.toBinaryString(0L, false), PgStrings.toBinaryString(-1L, false), PgStrings.toBinaryString(Long.MIN_VALUE, false), PgStrings.toBinaryString(Long.MAX_VALUE, false), null}};
        testType(columnName, pgType, array, id);

        array = new BitSet[][]{{BitSet.valueOf(new long[]{-1L})}};
        testType(columnName, pgType, array, id);


    }

    /**
     * @see PgType#VARBIT_ARRAY
     */
    final void doVarBitArrayBindAndExtract() {
        String columnName;
        final long id = startId + 53;
        final PgType pgType = PgType.VARBIT_ARRAY;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_varbit_array";
        testType(columnName, pgType, null, id);

        array = new Long[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][].class);


        array = new long[]{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);

        array = new Long[]{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, null};
        testType(columnName, pgType, array, id);

        array = new String[]{PgStrings.toBinaryString(0L, false), PgStrings.toBinaryString(-1L, false), PgStrings.toBinaryString(Long.MIN_VALUE, false), PgStrings.toBinaryString(Long.MAX_VALUE, false), null};
        testType(columnName, pgType, array, id);

        array = new BitSet[]{BitSet.valueOf(new long[]{-1L})};
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_varbit_array_2";
        testType(columnName, pgType, null, id);

        array = new Long[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[].class);
        row.getNonNull(columnName, Long[][].class);

        array = new long[][]{{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Long[][].class);

        array = new Long[][]{{0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, null}
                , {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, null}};
        testType(columnName, pgType, array, id);

        array = new String[][]{{PgStrings.toBinaryString(0L, false), PgStrings.toBinaryString(-1L, false), PgStrings.toBinaryString(Long.MIN_VALUE, false), PgStrings.toBinaryString(Long.MAX_VALUE, false), null}};
        testType(columnName, pgType, array, id);

        array = new BitSet[][]{{BitSet.valueOf(new long[]{-1L}), BitSet.valueOf(new long[]{Long.MAX_VALUE})}};
        testType(columnName, pgType, array, id);


    }

    /**
     * @see PgType#INTERVAL_ARRAY
     */
    final void doIntervalArrayBindAndExtract() {
        String columnName;
        final long id = startId + 54;
        final PgType pgType = PgType.INTERVAL_ARRAY;

        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_interval_array";
        testType(columnName, pgType, null, id);

        array = new Interval[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Interval[].class);
        row.getNonNull(columnName, Interval[][].class);

        array = new Interval[]{Interval.parse("PT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Duration[].class);

        array = new Interval[]{Interval.parse("P1DT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null};
        testType(columnName, pgType, array, id);

        array = new Duration[]{Duration.ofSeconds(343, 9999), Duration.ofSeconds(343, 9999).negated(), Duration.ZERO, null};
        testType(columnName, pgType, array, id);

        array = new Period[]{Period.ZERO, Period.of(1, 3, 4), null};
        testType(columnName, pgType, array, id);


        // below two dimension array
        columnName = "my_interval_array_2";
        testType(columnName, pgType, null, id);

        array = new Interval[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Interval[].class);
        row.getNonNull(columnName, Interval[][].class);

        array = new Interval[][]{{Interval.parse("PT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null}
                , {Interval.parse("PT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null}};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Duration[][].class);

        array = new Interval[][]{{Interval.parse("P1DT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null}
                , {Interval.parse("P1DT-0.999999S", true), Interval.parse("PT1M-60.9S", true), Interval.parse("PT2M-121.9S", true), Interval.ZERO, null}};
        testType(columnName, pgType, array, id);

        array = new Duration[][]{{Duration.ofSeconds(343, 9999), Duration.ofSeconds(343, 9999).negated(), Duration.ZERO, null}
                , {Duration.ofSeconds(343, 9999), Duration.ofSeconds(343, 9999).negated(), Duration.ZERO, null}};
        testType(columnName, pgType, array, id);

        array = new Period[][]{{Period.ZERO, Period.of(1, 3, 4), null}, {Period.ZERO, Period.of(1, 3, 4), null}};
        testType(columnName, pgType, array, id);

    }

    /**
     * @see PgType#BYTEA_ARRAY
     */
    final void doByteaArrayBindAndExtract() {
        String columnName;
        final long id = startId + 55;
        final PgType pgType = PgType.BYTEA_ARRAY;
        final String[] textArray = new String[]{"Set 'army' \\''", " '\"'"};
        final Charset charset = StandardCharsets.UTF_8;
        Object array;
        ResultRow row;

        // below one dimension array
        columnName = "my_bytea_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);

        row.getNonNull(columnName, byte[][].class);
        row.getNonNull(columnName, LongBinary[].class);
        row.getNonNull(columnName, byte[][][].class);
        row.getNonNull(columnName, LongBinary[][].class);

        array = new byte[][]{null};
        testType(columnName, pgType, array, id);

        row.getNonNull(columnName, byte[][].class);
        row.getNonNull(columnName, LongBinary[].class);
        row.getNonNull(columnName, byte[][][].class);
        row.getNonNull(columnName, LongBinary[][].class);


        array = textArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LongBinary[].class);

        array = new byte[][]{textArray[0].getBytes(charset), textArray[1].getBytes(charset)};
        testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LongBinary[].class);

        // below two dimension array
        columnName = "my_bytea_array_2";
        testType(columnName, pgType, null, id);

        array = new String[][]{null};
        row = testType(columnName, pgType, array, id);

        row.getNonNull(columnName, byte[][].class);
        row.getNonNull(columnName, LongBinary[].class);
        row.getNonNull(columnName, byte[][][].class);
        row.getNonNull(columnName, LongBinary[][].class);

        array = new byte[][][]{null};
        testType(columnName, pgType, array, id);

        row.getNonNull(columnName, byte[][].class);
        row.getNonNull(columnName, LongBinary[].class);
        row.getNonNull(columnName, byte[][][].class);
        row.getNonNull(columnName, LongBinary[][].class);


        array = new String[][]{textArray, textArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LongBinary[][].class);

        array = new byte[][][]{{textArray[0].getBytes(charset), textArray[1].getBytes(charset)}, {textArray[0].getBytes(charset), textArray[1].getBytes(charset)}};
        testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LongBinary[][].class);

    }

    /**
     * @see PgType#MONEY_ARRAY
     */
    final void doMoneyArrayBindAndExtract() {
        String columnName;
        final long id = startId + 56;
        final PgType pgType = PgType.MONEY_ARRAY;
        Object array;
        ResultRow row;

        final BigDecimal[] decimalArray = new BigDecimal[]{BigDecimal.ONE, BigDecimal.ZERO
                , new BigDecimal("-92233720368547758.08"), new BigDecimal("+92233720368547758.07")
                , null};
        final String[] moneyArray = new String[decimalArray.length];
        final DecimalFormat format = PgNumbers.getMoneyFormat(getDefaultLcMonetary());
        Objects.requireNonNull(format);

        for (int i = 0; i < decimalArray.length; i++) {
            if (decimalArray[i] == null) {
                moneyArray[i] = null;
                continue;
            }
            moneyArray[i] = format.format(decimalArray[i]);
            log.debug("moneyArray[{}]:{}", i, moneyArray[i]);
        }

        // below one dimension array
        columnName = "my_money_array";
        testType(columnName, pgType, null, id);

        array = new BigDecimal[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][].class);

        array = decimalArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        array = moneyArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);

        // below two dimension array
        columnName = "my_money_array_2";
        testType(columnName, pgType, null, id);

        array = new BigDecimal[][]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[].class);
        row.getNonNull(columnName, BigDecimal[][].class);

        array = new BigDecimal[][]{decimalArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);

        array = new String[][]{moneyArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, BigDecimal[][].class);

    }

    /**
     * @see PgType#VARCHAR_ARRAY
     */
    final void doVarCharArrayBindAndExtract() {
        String columnName;
        final long id = startId + 57;
        final PgType pgType = PgType.VARCHAR_ARRAY;
        Object array;
        ResultRow row;
        final String[] textArray = new String[]{"\\\\\\SET name = 'army' , { } } \\ ' \" \"QinArmy\"", "\n \" ' \033", null};

        // below one dimension array
        columnName = "my_varchar_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = textArray;
        testType(columnName, pgType, array, id);


        // below two dimension array
        columnName = "my_varchar_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, String[][][].class);

        array = new String[][]{textArray, textArray, textArray};
        testType(columnName, pgType, array, id);


        // below four dimension array
        columnName = "my_varchar_array_4";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][][][].class);
        row.getNonNull(columnName, String[][][][][].class);

        array = new String[][][][]{{{textArray, textArray}, {textArray, textArray}}, {{textArray, textArray}, {textArray, textArray}}};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#CHAR_ARRAY
     */
    final void doCharArrayBindAndExtract() {
        String columnName;
        final long id = startId + 58;
        final PgType pgType = PgType.CHAR_ARRAY;
        Object array;
        ResultRow row;
        // text can't end with whitespace for test
        final String[] textArray = new String[]{"\\\\\\SET name = 'army' , { } } \\ ' \" \"QinArmy\"", "\n \" ' \033", null};

        // below one dimension array
        columnName = "my_char_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = textArray;
        testType(columnName, pgType, array, id);


        // below two dimension array
        columnName = "my_char_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, String[][][].class);

        array = new String[][]{textArray, textArray, textArray};
        testType(columnName, pgType, array, id);


    }

    /**
     * @see PgType#TEXT_ARRAY
     */
    final void doTextArrayBindAndExtract() {
        String columnName;
        final long id = startId + 59;
        final PgType pgType = PgType.TEXT_ARRAY;
        Object array;
        ResultRow row;
        final String[] textArray = new String[]{"\\\\\\SET name = 'army' , { } } \\ ' \" \"QinArmy\"", "\n \" ' \033", null};

        // below one dimension array
        columnName = "my_text_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = textArray;
        testType(columnName, pgType, array, id);


        // below two dimension array
        columnName = "my_text_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, String[][][].class);

        array = new String[][]{textArray, textArray, textArray};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#JSON_ARRAY
     */
    final void doJsonArrayBindAndExtract() throws IOException {
        String columnName;
        final long id = startId + 60;
        final PgType pgType = PgType.JSON_ARRAY;
        Object array;
        ResultRow row;
        final String[] jsonArray = new String[4];
        final ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < jsonArray.length; i++) {
            Map<String, Object> map = new HashMap<>(4);
            map.put("id", i);
            map.put("name", "\\\\\\SET name = 'army' , { } } \\ \033 ' \" \"QinArmy\"");
            jsonArray[i] = mapper.writeValueAsString(map);
        }

        // below one dimension array
        columnName = "my_json_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = jsonArray;
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_json_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = new String[][]{jsonArray, jsonArray, jsonArray, jsonArray};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#XML_ARRAY
     */
    final void doXmlArrayBindAndExtract() {
        String columnName;
        final long id = startId + 61;
        final PgType pgType = PgType.XML_ARRAY;
        Object array;
        ResultRow row;
        final String[] xmlArray = new String[]{"<foo>QinArmy</foo>", "<foo>army</foo>", "<foo>zoro</foo>"};
        // below one dimension array
        columnName = "my_xml_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, String[][].class);

        array = xmlArray;
        testType(columnName, pgType, array, id);

        // below one dimension array
        columnName = "my_xml_array_2";
        array = new String[][]{xmlArray, xmlArray, xmlArray, xmlArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LongString[][].class);
    }


    final void doGenderArrayBindAndExtract() {
        String columnName;
        final long id = startId + 62;
        final PgType pgType = PgType.VARCHAR_ARRAY;
        Object array;
        ResultRow row;

        final Gender[] genderArray = new Gender[]{Gender.FEMALE, Gender.MALE, Gender.UNKNOWN};
        // below one dimension array
        columnName = "my_gender_array";
        testType(columnName, pgType, null, id);

        array = new Gender[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Gender[].class);
        row.getNonNull(columnName, Gender[][].class);

        array = genderArray;
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_gender_array_2";
        testType(columnName, pgType, null, id);

        array = new Gender[][]{genderArray, genderArray, genderArray};
        testType(columnName, pgType, array, id);

    }

    /**
     * @see PgType#UUID_ARRAY
     */
    final void doUuidArrayBindAndExtract() {
        String columnName;
        final long id = startId + 63;
        final PgType pgType = PgType.UUID_ARRAY;
        Object array;
        ResultRow row;

        final UUID[] uuidArray = new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        // below one dimension array
        columnName = "my_uuid_array";
        testType(columnName, pgType, null, id);

        array = new UUID[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, UUID[].class);
        row.getNonNull(columnName, UUID[][].class);

        array = uuidArray;
        testType(columnName, pgType, array, id);

        // below two dimension array
        columnName = "my_uuid_array_2";
        testType(columnName, pgType, null, id);

        array = new UUID[][]{uuidArray, uuidArray, uuidArray, uuidArray, uuidArray};
        testType(columnName, pgType, array, id);
    }

    /**
     * @see PgType#POINT_ARRAY
     */
    final void doPointArrayBindAndExtract() {
        String columnName;
        final long id = startId + 64;
        final PgType pgType = PgType.POINT_ARRAY;
        Object array;
        ResultRow row;

        final String[] pointArray = new String[]{"(0,0)", String.format("(%s,%s)", Double.MAX_VALUE, Double.MIN_VALUE), "(323.3,43432.36)"};

        // below one dimension array
        columnName = "my_point_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Point[].class);
        row.getNonNull(columnName, Point[][].class);

        array = pointArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        // below two dimension array
        columnName = "my_point_array_2";
        testType(columnName, pgType, null, id);

        array = new String[][]{pointArray, pointArray, pointArray, pointArray, pointArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
    }


    /**
     * @see PgType#LINE_ARRAY
     */
    final void doLineArrayBindAndExtract() {
        String columnName;
        final long id = startId + 65;
        final PgType pgType = PgType.LINE_ARRAY;
        Object array;
        ResultRow row;

        final String[] lineArray = new String[]{"{1.0,-1.0,0.0}", "{5.799010112459083E306,-1.0,-1.739703033737725E307}"};

        // below one dimension array
        columnName = "my_line_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, PgLine[].class);
        row.getNonNull(columnName, PgLine[][].class);

        array = lineArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        // below two dimension array
        columnName = "my_line_array_2";
        testType(columnName, pgType, null, id);

        array = new String[][]{lineArray, lineArray, lineArray, lineArray, lineArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
    }


    /**
     * @see PgType#LSEG_ARRAY
     */
    final void doLineSegmentArrayBindAndExtract() {
        String columnName;
        final long id = startId + 66;
        final PgType pgType = PgType.LSEG_ARRAY;
        Object array;
        ResultRow row;

        final String[] lineSegmentArray = new String[]{"[(0,0),(1,1)]", String.format("[(34.33,%s),(44.33,%s)]", Double.MAX_VALUE, Double.MIN_VALUE)};

        // below one dimension array
        columnName = "my_line_segment_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, Line[].class);
        row.getNonNull(columnName, Line[][].class);

        array = lineSegmentArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        // below two dimension array
        columnName = "my_line_segment_array_2";
        testType(columnName, pgType, null, id);

        array = new String[][]{lineSegmentArray, lineSegmentArray, lineSegmentArray, lineSegmentArray, lineSegmentArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
    }

    /**
     * @see PgType#BOX_ARRAY
     */
    final void doBoxArrayBindAndExtract() {
        String columnName;
        final long id = startId + 67;
        final PgType pgType = PgType.BOX_ARRAY;
        Object array;
        ResultRow row;

        final String[] boxArray = new String[]{"(0,0),(1,1)", String.format("(3454.3,%s),(3456.334,%s)", Double.MIN_VALUE, Double.MAX_VALUE)};

        // below one dimension array
        columnName = "my_box_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, PgBox[].class);
        row.getNonNull(columnName, PgBox[][].class);

        array = boxArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        // below two dimension array
        columnName = "my_box_array_2";
        testType(columnName, pgType, null, id);

        array = new String[][]{boxArray, boxArray, boxArray, boxArray, boxArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
    }

    /**
     * @see PgType#PATH_ARRAY
     */
    final void doPathArrayBindAndExtract() {
        String columnName;
        final long id = startId + 68;
        final PgType pgType = PgType.PATH_ARRAY;
        Object array;
        ResultRow row;

        final String[] pathArray = new String[]{"[(0,0),(1,1),(2,2)]", "((0,0),(1,1),(2,2))", String.format("[(0,%s),(1,1),(2,%s)]", Double.MAX_VALUE, Double.MIN_VALUE), String.format("((0,%s),(1,1),(2,%s))", Double.MAX_VALUE, Double.MIN_VALUE)};

        // below one dimension array
        columnName = "my_path_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LineString[].class);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, LongString[].class);

        array = pathArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, LongString[].class);

        // below two dimension array
        columnName = "my_path_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, LineString[][].class);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, LongString[][].class);

        array = new String[][]{pathArray, pathArray, pathArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, LongString[][].class);

    }


    /**
     * @see PgType#POLYGON_ARRAY
     */
    final void doPolygonArrayBindAndExtract() {
        String columnName;
        final long id = startId + 69;
        final PgType pgType = PgType.POLYGON_ARRAY;
        Object array;
        ResultRow row;

        final String[] polygonArray = new String[]{"((0,0),(1,1),(2,2))", String.format("((0,%s),(1,1),(2,%s))", Double.MAX_VALUE, Double.MIN_VALUE)};

        // below one dimension array
        columnName = "my_polygon_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, PgPolygon[].class);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, LongString[].class);

        array = polygonArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);
        row.getNonNull(columnName, LongString[].class);

        // below two dimension array
        columnName = "my_polygon_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, PgPolygon[][].class);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, LongString[][].class);

        array = new String[][]{polygonArray, polygonArray, polygonArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);
        row.getNonNull(columnName, LongString[][].class);

    }

    /**
     * @see PgType#CIRCLE_ARRAY
     */
    final void doCirclesArrayBindAndExtract() {
        String columnName;
        final long id = startId + 70;
        final PgType pgType = PgType.CIRCLE_ARRAY;
        Object array;
        ResultRow row;

        final String[] circlesArray = new String[]{"<(0,0),5.3>", String.format("<(%s,%s),8.8>", Double.MIN_VALUE, Double.MAX_VALUE)};

        // below one dimension array
        columnName = "my_circles_array";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        array = circlesArray;
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);


        // below two dimension array
        columnName = "my_circles_array_2";
        testType(columnName, pgType, null, id);

        array = new String[]{null};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[].class);

        array = new String[][]{circlesArray, circlesArray, circlesArray};
        row = testType(columnName, pgType, array, id);
        row.getNonNull(columnName, String[][].class);

    }

    /**
     * @see PgType#TEXT
     * @see PgType#JSON
     */
    final void doPathParameterBindAndExtract() throws IOException {
        String columnName;
        final long id = startId + 71;
        ResultRow row;

        final Path path = createBigJsonFile();

        try {
            columnName = "my_text";
            row = testType(columnName, PgType.TEXT, path, id);
            assertEquals(row.get(columnName, String.class), PgStreams.readAsString(path), columnName);

            columnName = "my_json";
            row = testType(columnName, PgType.JSON, path, id);
            assertEquals(row.get(columnName, String.class), PgStreams.readAsString(path), columnName);
        } finally {
            Files.deleteIfExists(path);
        }

    }

    /**
     * @see PgType#TEXT
     * @see PgType#JSON
     */
    final void doPublisherParameterBindAndExtract() throws IOException {
        String columnName;
        final long id = startId + 71;
        ResultRow row;

        final Path path = createBigJsonFile();
        try {
            columnName = "my_text";
            row = testType(columnName, PgType.TEXT, new PathParameterPublisher(path), id);
            assertEquals(row.get(columnName, String.class), PgStreams.readAsString(path), columnName);

            columnName = "my_json";
            row = testType(columnName, PgType.JSON, new PathParameterPublisher(path), id);
            assertEquals(row.get(columnName, String.class), PgStreams.readAsString(path), columnName);
        } finally {
            Files.deleteIfExists(path);
        }

    }


    private static final class PathParameterPublisher implements Publisher<byte[]> {

        private final Path path;

        private PathParameterPublisher(Path path) {
            this.path = path;
        }

        @Override
        public final void subscribe(Subscriber<? super byte[]> s) {
            s.onSubscribe(new PathSubscription(this.path, s));
        }

    }

    private static final class PathSubscription implements Subscription {

        private static final AtomicIntegerFieldUpdater<PathSubscription> STATUS = AtomicIntegerFieldUpdater
                .newUpdater(PathSubscription.class, "status");

        private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(2);

        private final Path path;

        private final Subscriber<? super byte[]> subscriber;

        private volatile int status = 0;

        private PathSubscription(Path path, Subscriber<? super byte[]> subscriber) {
            this.path = path;
            this.subscriber = subscriber;
        }

        @Override
        public final void request(long n) {
            if (STATUS.compareAndSet(this, 0, 1)) {
                EXECUTOR_SERVICE.schedule(this::publishData, 2, TimeUnit.SECONDS);
            }
        }

        @Override
        public final void cancel() {
            STATUS.compareAndSet(this, 1, 2);
        }

        private void publishData() {
            try (InputStream in = Files.newInputStream(this.path, StandardOpenOption.READ)) {
                final byte[] buffer = new byte[256];
                int length;
                while ((length = in.read(buffer)) > 0) {
                    if (length == buffer.length) {
                        this.subscriber.onNext(Arrays.copyOf(buffer, length));
                    } else {
                        this.subscriber.onNext(Arrays.copyOfRange(buffer, 0, length));
                    }
                    if (STATUS.get(this) == 2) {
                        return;
                    }
                }
                this.subscriber.onComplete();
                STATUS.compareAndSet(this, 1, 3);
            } catch (Throwable e) {
                this.subscriber.onError(e);
                STATUS.compareAndSet(this, 1, 4);
            }
        }


    }

    final Path createBigJsonFile() throws IOException {
        final Path path = Files.createTempFile("bigFile", ".json");
        try (Writer writer = Files.newBufferedWriter(path, StandardOpenOption.WRITE)) {
            final int count = 1000;
            final Map<String, Object> map = new HashMap<>((int) (count / 0.75F));
            for (int i = 0; i < 1000; i++) {
                map.put(Integer.toString(i), "'army' \\:" + i);
            }
            final ObjectMapper mapper = new ObjectMapper();
            writer.write(mapper.writeValueAsString(map));
        } catch (Throwable e) {
            Files.deleteIfExists(path);
        }
        return path;
    }


    private ResultRow testType(final String columnName, final PgType columnType
            , final @Nullable Object value, final long id) {
        assertNotEquals(columnName, "id", "can't be id");

        //1. update column
        final String updateSql = String.format("UPDATE my_types AS t SET %s = ? WHERE t.id = ?", columnName);

        final List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, columnType, value));
        paramGroup.add(BindValue.wrap(1, PgType.BIGINT, id));

        final PgProtocol protocol;
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
        row = executeQuery(PgStmts.single(querySql, BindValue.wrap(0, PgType.BIGINT, id)), adjutant)

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
        } else if (!(value instanceof Path || value instanceof Publisher)) {
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
            case FLOAT8: {
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
            case LSEG: {
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
            case CIRCLE: {
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
            case TIMESTAMPTZ_ARRAY:
                assertTimestampTzArray(columnName, row, nonNull);
                break;
            case TIMETZ_ARRAY:
                assertTimeTzArray(columnName, row, nonNull);
                break;
            case INTERVAL_ARRAY:
                assertIntervalArray(columnName, row, nonNull);
                break;
            case MONEY_ARRAY:
                assertMoneyArray(columnName, row, nonNull);
                break;
            case CHAR_ARRAY:
                assertCharArray(columnName, row, nonNull);
                break;
            case POINT_ARRAY:
                assertPointArray(columnName, row, nonNull);
                break;
            case LINE_ARRAY:
                assertLineArray(columnName, row, nonNull);
                break;
            case LSEG_ARRAY:
                assertLineSegmentArray(columnName, row, nonNull);
                break;
            case BOX_ARRAY:
                assertBoxArray(columnName, row, nonNull);
                break;
            case PATH_ARRAY:
                assertPathArray(columnName, row, nonNull);
                break;
            case POLYGON_ARRAY:
                assertPolygonArray(columnName, row, nonNull);
                break;
            case CIRCLE_ARRAY:
                assertCirclesArray(columnName, row, nonNull);
                break;
            case TSVECTOR_ARRAY:
            case TSQUERY_ARRAY:
                // assert by test method
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

    private void assertCirclesArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, Circle[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, Circle[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.circle((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertPolygonArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, PgPolygon[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, PgPolygon[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.polygon((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertPathArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, LineString[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, LineString[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.path((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertBoxArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, PgBox[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, PgBox[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.box((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertPointArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, Point[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, Point[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.point((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertLineSegmentArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, Line[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, Line[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.lineSegment((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertLineArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(nonNull.getClass());
        final Object result;
        switch (pair.getSecond()) {
            case 1:
                result = row.getNonNull(columnName, PgLine[].class);
                break;
            case 2:
                result = row.getNonNull(columnName, PgLine[][].class);
                break;
            default:
                throw new IllegalArgumentException("not unknown dimensions");
        }

        final BiPredicate<Object, Object> function = (value, resultValue) -> PgGeometries.line((String) value).equals(resultValue);
        assertArray(columnName, nonNull, result, function);
    }

    private void assertCharArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Object result = row.get(columnName, nonNull.getClass());
        assertTrue(nonNull.getClass().isInstance(result), columnName);
        final BiPredicate<Object, Object> function = (value, resultValue) -> {
            String v = (String) value, r = (String) resultValue;
            final int length = r.length();
            for (int i = length - 1; i > -1; i--) {
                if (!Character.isWhitespace(r.charAt(i))) {
                    r = r.substring(0, i + 1);
                    break;
                }
            }
            return v.equals(r);
        };
        assertArray(columnName, nonNull, result, function);
    }

    private void assertMoneyArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Object result = row.get(columnName, nonNull.getClass());
        assertTrue(nonNull.getClass().isInstance(result), columnName);
        final BiPredicate<Object, Object> function = (value, resultValue) -> {
            final boolean match;
            if (value instanceof BigDecimal) {
                match = ((BigDecimal) value).compareTo((BigDecimal) resultValue) == 0;
            } else if (value instanceof String) {
                String v = (String) value, r = (String) resultValue;
                if (v.endsWith(".00") || v.equals(".0")) {
                    v = v.substring(0, v.length() - 3);
                }
                if (r.endsWith(".00") || r.equals(".0")) {
                    r = r.substring(0, r.length() - 3);
                }
                match = v.equals(r);
            } else {
                throw new IllegalArgumentException(String.format("value type[%s] unknown.", value.getClass().getName()));
            }
            if (!match) {
                log.debug("money array value:{},money result value:{}", value, resultValue);
            }
            return match;
        };
        assertArray(columnName, nonNull, result, function);
    }

    private void assertIntervalArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Object result = row.get(columnName, nonNull.getClass());
        assertTrue(nonNull.getClass().isInstance(result), columnName);
        final BiPredicate<Object, Object> function = (value, resultValue) -> {
            final boolean match;
            if (value instanceof Interval) {
                match = ((Interval) value).equals(resultValue, true);
            } else if (value instanceof Duration) {
                match = Interval.of((Duration) value).equals(Interval.of((Duration) value), true);
            } else if (value instanceof Period) {
                match = Interval.of((Period) value).equals(Interval.of((Period) value), true);
            } else {
                throw new IllegalArgumentException(String.format("value type[%s] unknown.", value.getClass().getName()));
            }
            return match;
        };
        assertArray(columnName, nonNull, result, function);
    }

    private void assertTimeTzArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Object result = row.get(columnName, nonNull.getClass());
        assertTrue(nonNull.getClass().isInstance(result), columnName);
        final BiPredicate<Object, Object> function = (value, resultValue) -> {
            final OffsetTime v = (OffsetTime) value, r = (OffsetTime) resultValue;
            return v.equals(r.withOffsetSameInstant(v.getOffset()));
        };

        assertArray(columnName, nonNull, result, function);
    }

    private void assertTimestampTzArray(final String columnName, final ResultRow row, final Object nonNull) {
        final Object result = row.get(columnName, nonNull.getClass());
        assertTrue(nonNull.getClass().isInstance(result), columnName);
        final BiPredicate<Object, Object> function = (value, resultValue) -> {
            final boolean match;
            if (value instanceof OffsetDateTime) {
                final OffsetDateTime v = (OffsetDateTime) value;
                final OffsetDateTime r = ((OffsetDateTime) resultValue).withOffsetSameInstant(v.getOffset());
                match = v.equals(r);
            } else if (value instanceof ZonedDateTime) {
                final ZonedDateTime v = (ZonedDateTime) value;
                if (resultValue instanceof OffsetDateTime) {
                    final ZonedDateTime r = ((OffsetDateTime) resultValue).withOffsetSameInstant(v.getOffset())
                            .toZonedDateTime();
                    match = v.equals(r);
                } else if (resultValue instanceof ZonedDateTime) {
                    final ZonedDateTime r = ((ZonedDateTime) resultValue).withZoneSameInstant(v.getZone());
                    match = v.equals(r);
                } else {
                    String m = String.format("columnName[%s] type[%s] unknown.", columnName, value.getClass().getName());
                    throw new IllegalArgumentException(m);
                }

            } else if (value instanceof String) {
                match = ((String) value).equalsIgnoreCase((String) resultValue);
            } else {
                String m = String.format("columnName[%s] type[%s] unknown.", columnName, value.getClass().getName());
                throw new IllegalArgumentException(m);
            }
            return match;
        };

        assertArray(columnName, nonNull, result, function);

    }

    private void assertArray(final String columnName, final Object array
            , final Object resultArray, final BiPredicate<Object, Object> predicate) {

        final int arrayLength = Array.getLength(array), resultLength = Array.getLength(resultArray);
        assertEquals(arrayLength, resultLength, columnName);
        Object value, resultValue;
        for (int i = 0; i < arrayLength; i++) {
            value = Array.get(array, i);
            resultValue = Array.get(resultArray, i);
            if (value == null) {
                assertNull(resultValue, columnName);
                continue;
            }
            assertNotNull(resultValue, columnName);
            if (value.getClass().isArray()) {
                assertArray(columnName, value, resultValue, predicate);
            } else {
                assertTrue(predicate.test(value, resultValue), columnName);
            }

        }

    }


}
