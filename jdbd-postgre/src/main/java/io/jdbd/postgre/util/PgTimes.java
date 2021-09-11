package io.jdbd.postgre.util;

import io.jdbd.vendor.util.JdbdTimes;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
 */
public abstract class PgTimes extends JdbdTimes {

    /**
     * <p>
     * <pre>
     *         Low Value        High Value
     *         ---------        ----------
     *         4713 BC          5874897 AD
     *     </pre>
     * </p>
     */
    public static final DateTimeFormatter PG_ISO_LOCAL_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 7, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, TextStyle.SHORT)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    /**
     * <p>
     * <pre>
     *         Low Value        High Value
     *         ---------        ----------
     *         4713 BC          294276 AD
     *     </pre>
     * </p>
     */
    public static final DateTimeFormatter PG_ISO_LOCAL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 6, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .append(PgTimes.ISO_LOCAL_TIME_FORMATTER)
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, TextStyle.SHORT)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    /**
     * <p>
     * <pre>
     *         Low Value        High Value
     *         ---------        ----------
     *         4713 BC          294276 AD
     *     </pre>
     * </p>
     */
    public static final DateTimeFormatter PG_ISO_OFFSET_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 6, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .append(PgTimes.ISO_OFFSET_TIME_FORMATTER)
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, TextStyle.SHORT)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    /**
     * Only use to parse output
     */
    private static final DateTimeFormatter PG_ISO_OFFSET_HH_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_TIME_FORMATTER)
            .appendOffset("+HH:mm", "+00:00")
            .toFormatter(Locale.ENGLISH);

    /**
     * Only use to parse output
     * <p>
     * <pre>
     *         Low Value        High Value
     *         ---------        ----------
     *         4713 BC          294276 AD
     *     </pre>
     * </p>
     */
    private static final DateTimeFormatter PG_ISO_OFFSET_HH_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 6, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .append(PgTimes.ISO_LOCAL_TIME_FORMATTER)
            .appendOffset("+HH:mm", "+00:00")
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, TextStyle.SHORT)
            .optionalEnd()
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


    public static OffsetDateTime parseIsoOffsetDateTime(final String textValue) throws DateTimeException {

        OffsetDateTime dateTime;
        try {
            dateTime = OffsetDateTime.parse(textValue, PG_ISO_OFFSET_DATETIME_FORMATTER);
        } catch (DateTimeException e) {
            // postgre iso zone offset output is too too too too too too too too too too too stupid.
            // +HH or +HH:MM or +HH:MM:SS
            try {
                dateTime = OffsetDateTime.parse(textValue, PG_ISO_OFFSET_HH_DATETIME_FORMATTER);
            } catch (DateTimeException e2) {
                throw e;
            }
        }
        return dateTime;
    }

    public static OffsetTime parseIsoOffsetTime(String textValue) throws DateTimeException {
        OffsetTime time;
        try {
            time = OffsetTime.parse(textValue, ISO_OFFSET_TIME_FORMATTER);
        } catch (DateTimeException e) {
            // postgre iso zone offset output is too too too too too too too too too too too stupid.
            // +HH or +HH:MM or +HH:MM:SS
            try {
                time = OffsetTime.parse(textValue, PG_ISO_OFFSET_HH_TIME_FORMATTER);
            } catch (DateTimeException e2) {
                throw e;
            }
        }
        return time;
    }


}
