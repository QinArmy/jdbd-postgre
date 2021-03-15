package io.jdbd.mysql.util;

import io.jdbd.mysql.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;
import static org.testng.Assert.*;

@Test(groups = {Groups.UTILS})
public class MySQLTimeUtilsSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLTimeUtilsSuiteTests.class);

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n {} test start\n", Groups.UTILS);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n {} test end\n", Groups.UTILS);
    }


    @Test
    public void parseTimeAsDuration() {
        LOG.info("parseTimeAsDuration test start");

        long totalSecond, totalNanos;

        String timeText = "838:59:59";
        totalSecond = 838L * 3600L + 59 * 60 + 59;
        totalNanos = 0L;
        Duration duration = MySQLTimeUtils.parseTimeAsDuration(timeText);
        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "838:59:59.000000";
        totalSecond = 838L * 3600L + 59 * 60 + 59;
        totalNanos = 0L;

        duration = MySQLTimeUtils.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "-838:59:59";
        totalSecond = -838L * 3600L - 59 * 60 - 59;
        totalNanos = 0L;

        duration = MySQLTimeUtils.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertTrue(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "-838:59:59.000000";
        totalSecond = -838L * 3600L - 59 * 60 - 59;
        totalNanos = 0L;

        duration = MySQLTimeUtils.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertTrue(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");


        timeText = "23:59:59.999999";
        totalSecond = 23L * 3600L + 59L * 60L + 59L;
        totalNanos = 999_999L * 1000L;

        duration = MySQLTimeUtils.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertFalse(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");


        LOG.info("parseTimeAsDuration test end");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText1() {
        LOG.info("errorDurationText1 test start");

        MySQLTimeUtils.parseTimeAsDuration("343:-34:34");

        fail("errorDurationText1 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText2() {
        LOG.info("errorDurationText2 test start");

        MySQLTimeUtils.parseTimeAsDuration("839:34:34");

        fail("errorDurationText2 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText3() {
        LOG.info("errorDurationText3 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:60:34");

        fail("errorDurationText3 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText4() {
        LOG.info("errorDurationText4 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:59:60");

        fail("errorDurationText4 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText5() {
        LOG.info("errorDurationText4 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:59:59.9999999");

        fail("errorDurationText4 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText6() {
        LOG.info("errorDurationText6 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:59:-59.999999");

        fail("errorDurationText6 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText7() {
        LOG.info("errorDurationText7 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:59:59.-9999");

        fail("errorDurationText7 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText8() {
        LOG.info("errorDurationText8 test start");

        MySQLTimeUtils.parseTimeAsDuration("33:59:59.9999999");

        fail("errorDurationText8 failure.");
    }

    /**
     * test {@link MySQLTimeUtils#convertToDuration(LocalTime)}
     */
    @Test
    public void convertLocalTimeToDuration() {
        LOG.info("convertLocalTimeToDuration test start");

        String timeText = "23:59:59.999999";
        LocalTime time = LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER);

        Duration duration = MySQLTimeUtils.convertToDuration(time);
        long totalSeconds, totalNano;
        totalSeconds = 23L * 3600L + 59L * 60L + 59L;
        totalNano = 999_999L * 1000L;
        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSeconds, "totalSeconds");
        assertEquals(duration.getNano(), totalNano, "totalNano");

        timeText = "00:00:00.000000";
        time = LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        duration = MySQLTimeUtils.convertToDuration(time);

        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), 0L, "totalSeconds");
        assertEquals(duration.getNano(), 0L, "totalNano");

        LOG.info("convertLocalTimeToDuration test end");
    }

    /**
     * test {@link MySQLTimeUtils#convertToDuration(LocalTime)}
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void errorLocalTimeConvertToDuration() {
        LOG.info("errorLocalTimeConvertToDuration test start");

        String timeText = "23:59:59.999999999";

        final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)

                .optionalStart()
                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .toFormatter(Locale.ENGLISH);

        LocalTime time = LocalTime.parse(timeText, formatter);

        MySQLTimeUtils.convertToDuration(time);

        fail(String.format("errorLocalTimeConvertToDuration test failure ,time:%s ,nano:%s", time
                .format(formatter), time.getNano()));
    }


}
