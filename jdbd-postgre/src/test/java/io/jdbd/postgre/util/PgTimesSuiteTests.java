package io.jdbd.postgre.util;

import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static org.testng.Assert.assertEquals;

@Test(groups = {Group.UTILS})
public class PgTimesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgTimesSuiteTests.class);

    /**
     * @see PgTimes#parseIsoOffsetDateTime(String)
     */
    @Test
    public void parseIsoOffsetDateTime() {
        final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .append(PgTimes.ISO_LOCAL_DATETIME_FORMATTER)
                .appendOffset("+HH:MM:SS", "+00:00")
                .toFormatter(Locale.ENGLISH);

        final LocalDateTime dateTime = LocalDateTime.parse("2021-08-09 15:46:45", PgTimes.ISO_LOCAL_DATETIME_FORMATTER);

        assertIsoOffsetDateTimeParse("+00:00", formatter, dateTime);

        assertIsoOffsetDateTimeParse("+08:09:07", formatter, dateTime);
        assertIsoOffsetDateTimeParse("+08:09", formatter, dateTime);
        assertIsoOffsetDateTimeParse("+08", formatter, dateTime);

        assertIsoOffsetDateTimeParse("-08:09:07", formatter, dateTime);
        assertIsoOffsetDateTimeParse("-08:09", formatter, dateTime);
        assertIsoOffsetDateTimeParse("-08", formatter, dateTime);
    }

    /**
     * @see PgTimes#parseIsoOffsetDateTime(String)
     */
    @Test(expectedExceptions = DateTimeException.class)
    public void parseIsoOffsetDateTimeError() {
        PgTimes.parseIsoOffsetDateTime("2021-08-09 15:46:45");
    }

    /**
     * @see #parseIsoOffsetDateTime()
     */
    private static void assertIsoOffsetDateTimeParse(final String offsetText, final DateTimeFormatter formatter
            , final LocalDateTime dateTime) {
        ZoneOffset offset = ZoneOffset.of(offsetText);
        String textValue = OffsetDateTime.of(dateTime, offset).format(formatter);
        OffsetDateTime offsetDateTime = PgTimes.parseIsoOffsetDateTime(textValue);

        assertEquals(offsetDateTime.toLocalDateTime(), dateTime, offsetText);
        assertEquals(offsetDateTime.getOffset().normalized(), offset, offsetText);

    }

}
