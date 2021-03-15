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
     * @return true: timeText representing {@link Duration}.
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static boolean isDuration(final String timeText) {
        int index = timeText.indexOf(':');
        final boolean duration;
        if (index < 0) {
            duration = false;
        } else {
            final int hours;
            hours = Integer.parseInt(timeText.substring(0, index));
            duration = hours < 0 || hours > 23;
        }
        return duration;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     * @see #convertToDuration(LocalTime)
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
            if (hours < -838 || hours > 838
                    || minutes < 0 || minutes > 59
                    || seconds < 0 || seconds > 59
                    || micros < 0 || micros > 999_999) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            } else if (Math.abs(hours) == 838 && minutes == 59 && seconds == 59 && micros != 0) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            }
            final long totalSecond;
            if (hours < 0) {
                totalSecond = (hours * 3600L) - (minutes * 60L) - seconds;
            } else {
                totalSecond = (hours * 3600L) + (minutes * 60L) + seconds;
            }
            //nanoAdjustment must be positive ,java.time.Duration.ofSeconds(long, long) method invoke java.lang.Math.floorDiv(long, long) cause bug.
            return Duration.ofSeconds(totalSecond, micros * 1000L);
        } catch (Throwable e) {
            throw new DateTimeException(createTimeFormatErrorMessage(timeText), e);
        }


    }

    /**
     * @param time {@link LocalTime} that underlying {@link java.time.ZoneOffset} match with database.
     * @throws IllegalArgumentException throw when {@link LocalTime#getNano()} great than {@code 999_999_000}.
     * @see #parseTimeAsDuration(String)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static Duration convertToDuration(LocalTime time) throws IllegalArgumentException {
        final long totalSecond, nano;
        totalSecond = time.getHour() * 3600L + time.getMinute() * 60L + time.getSecond();
        nano = time.getNano();
        if (nano > 999_999_000) {
            throw new IllegalArgumentException("time can't convert to Duration.");
        }
        return Duration.ofSeconds(totalSecond, nano);
    }

    private static String createTimeFormatErrorMessage(final String timeText) {
        return String.format("MySQL TIME[%s] format error.", timeText);
    }


}
