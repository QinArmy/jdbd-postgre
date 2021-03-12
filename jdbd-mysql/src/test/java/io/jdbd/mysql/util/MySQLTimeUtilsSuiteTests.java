package io.jdbd.mysql.util;

import io.jdbd.mysql.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

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

        timeText = "838:59:59.999999";
        totalSecond = 838L * 3600L + 59 * 60 + 59;
        totalNanos = 999999L * 1000L;

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

        timeText = "-838:59:59.999999";
        totalSecond = -838L * 3600L - 59 * 60 - 59;
        totalNanos = 999_999L * 1000L;

        duration = MySQLTimeUtils.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertTrue(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        LOG.info("parseTimeAsDuration test end");
    }


}
