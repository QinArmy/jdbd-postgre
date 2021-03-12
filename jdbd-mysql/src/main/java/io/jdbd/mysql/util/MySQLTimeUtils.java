package io.jdbd.mysql.util;

import org.qinarmy.util.TimeUtils;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;

public abstract class MySQLTimeUtils extends TimeUtils {

    protected MySQLTimeUtils() {
        throw new UnsupportedOperationException();
    }

    /*################################## blow time formatter ##################################*/

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_0 = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)

            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_1 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 1, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_2 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 2, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_3 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_4 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 4, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_5 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 5, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    /*################################## blow datetime formatter ##################################*/

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_0 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_0)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_1 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_1)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_2 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_2)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_3 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_3)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_4 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_4)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_5 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_5)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER)
            .toFormatter(Locale.ENGLISH);


    /**
     * @return {@link java.time.LocalTime} or {@link Duration}
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static Object parseTime(final String timeText) throws DateTimeException {
        Object timeValue;
        try {
            timeValue = LocalTime.parse(timeText, MYSQL_TIME_FORMATTER);
        } catch (DateTimeException e) {
            timeValue = parseTimeAsDuration(timeText);
        }
        return timeValue;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static Duration parseTimeAsDuration(final String timeText) throws DateTimeException {

        try {
            final String[] itemArray = timeText.trim().split(":");
            if (itemArray.length != 3) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            }
            final int hours, minutes, seconds, micros;
            hours = Integer.parseInt(itemArray[0]);
            minutes = Integer.parseInt(itemArray[1]);

            if (itemArray[2].contains(".")) {
                String[] secondPartArray = itemArray[2].split("\\.");
                if (secondPartArray.length != 2) {
                    throw new DateTimeException(createTimeFormatErrorMessage(timeText));
                }
                seconds = Integer.parseInt(secondPartArray[0]);
                micros = Integer.parseInt(secondPartArray[1]);
            } else {
                seconds = Integer.parseInt(itemArray[2]);
                micros = 0;
            }
            if (minutes < 0 || seconds < 0 || micros < 0) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            }
            final long totalSecond;
            if (hours < 0) {
                totalSecond = (hours * 3600L) - (minutes * 60L) - seconds;
            } else {
                totalSecond = (hours * 3600L) + (minutes * 60L) + seconds;
            }
            return Duration.ofSeconds(totalSecond, micros * 1000L);
        } catch (NumberFormatException e) {
            throw new DateTimeException(createTimeFormatErrorMessage(timeText), e);
        }


    }

    private static String createTimeFormatErrorMessage(final String timeText) {
        return String.format("MySQL TIME[%s] format error.", timeText);
    }


}
