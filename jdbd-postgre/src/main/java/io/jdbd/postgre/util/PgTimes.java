package io.jdbd.postgre.util;

import io.jdbd.type.IntervalPair;
import io.jdbd.vendor.util.JdbdTimes;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAmount;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;

public abstract class PgTimes extends JdbdTimes {


    public static final DateTimeFormatter ISO_LOCAL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)

            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter ISO_LOCAL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_FORMATTER)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter ISO_OFFSET_DATETIME__FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATETIME_FORMATTER)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter ISO_OFFSET_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_TIME_FORMATTER)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter(Locale.ENGLISH);

    /**
     * Converts the given postgresql seconds to java seconds. Reverse engineered by inserting varying
     * dates to postgresql and tuning the formula until the java dates matched. See {@link #toPgSeconds}
     * for the reverse operation.
     *
     * @param seconds Postgresql seconds.
     * @return Java seconds.
     */
    public static long toJavaSeconds(long seconds) {
        // postgres epoc to java epoc
        seconds += 946684800L;

        // Julian/Gregorian calendar cutoff point
        if (seconds < -12219292800L) { // October 4, 1582 -> October 15, 1582
            seconds += 86400 * 10;
            if (seconds < -14825808000L) { // 1500-02-28 -> 1500-03-01
                int extraLeaps = (int) ((seconds + 14825808000L) / 3155760000L);
                extraLeaps--;
                extraLeaps -= extraLeaps / 4;
                seconds += extraLeaps * 86400L;
            }
        }
        return seconds;
    }

    /**
     * Converts the given java seconds to postgresql seconds. See {@link #toJavaSeconds} for the reverse
     * operation. The conversion is valid for any year 100 BC onwards.
     *
     * @param seconds Postgresql seconds.
     * @return Java seconds.
     */
    public static long toPgSeconds(long seconds) {
        // java epoc to postgres epoc
        seconds -= 946684800L;

        // Julian/Greagorian calendar cutoff point
        if (seconds < -13165977600L) { // October 15, 1582 -> October 4, 1582
            seconds -= 86400 * 10;
            if (seconds < -15773356800L) { // 1500-03-01 -> 1500-02-28
                int years = (int) ((seconds + 15773356800L) / -3155823050L);
                years++;
                years -= years / 4;
                seconds += years * 86400L;
            }
        }

        return seconds;
    }

    /**
     * @throws DateTimeParseException not iso_8601
     */
    public static TemporalAmount parseIsoInterval(final String textValue) {
        final TemporalAmount amount;
        int timeIndex;
        if ((timeIndex = textValue.indexOf('T')) < 0
                && (timeIndex = textValue.indexOf('t')) < 0) {
            amount = Period.parse(textValue);
        } else if (timeIndex == 1) {
            amount = Duration.parse(textValue);
        } else if (timeIndex > 1) {
            Period period = Period.parse(textValue.substring(0, timeIndex));
            Duration duration = Duration.parse("P" + textValue.substring(timeIndex));
            amount = IntervalPair.of(period, duration);
        } else {
            String m = String.format("TextValue[%s] isn't iso interval.", textValue);
            throw new DateTimeParseException(m, textValue, timeIndex);
        }
        return amount;
    }


}
