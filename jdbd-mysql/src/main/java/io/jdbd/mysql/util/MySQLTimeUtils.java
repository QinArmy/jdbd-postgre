package io.jdbd.mysql.util;

import org.qinarmy.util.TimeUtils;

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


}
