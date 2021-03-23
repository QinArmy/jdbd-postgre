package io.jdbd.mysql.protocol.client;

import io.jdbd.UnsupportedConvertingException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.result.AbstractResultRow;

import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

abstract class MySQLResultRow extends AbstractResultRow<MySQLRowMeta> {

    static MySQLResultRow from(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        return new SimpleMySQLResultRow(columnValues, rowMeta, adjutant);
    }

    private final ResultRowAdjutant adjutant;

    private MySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        super(rowMeta, columnValues);
        this.adjutant = adjutant;
    }


    /*################################## blow protected method ##################################*/


    @SuppressWarnings("unchecked")
    @Override
    protected <T> Set<T> convertNonNullToSet(final int indexBaseZero, final Object nonValue, final Class<T> elementClass)
            throws UnsupportedConvertingException {
        final Set<T> set;
        if (nonValue instanceof Set) {
            if (this.rowMeta.columnMetaArray[indexBaseZero].isSetType()) {
                Set<String> stringSet = (Set<String>) nonValue;
                if (elementClass == String.class) {
                    set = (Set<T>) stringSet;
                } else if (elementClass.isEnum()) {
                    Set<T> tempSet = MySQLStringUtils.convertStringsToEnumSet(stringSet, elementClass);
                    set = MySQLCollections.unmodifiableSet(tempSet);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
        } else {
            set = super.convertNonNullToSet(indexBaseZero, nonValue, elementClass);
        }
        return set;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonValue
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        final List<T> list;
        if (nonValue instanceof Set) {
            if (this.rowMeta.columnMetaArray[indexBaseZero].isSetType()) {
                if (elementClass == String.class) {
                    list = (List<T>) MySQLCollections.unmodifiableList(new ArrayList<>((Set<String>) nonValue));
                } else if (elementClass.isEnum()) {
                    List<T> tempList = MySQLStringUtils.convertStringsToEnumList((Set<String>) nonValue, elementClass);
                    list = MySQLCollections.unmodifiableList(tempList);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
        } else {
            list = super.convertNonNullToList(indexBaseZero, nonValue, elementClass);
        }
        return list;
    }

    /**
     * <p>
     * see {@code com.mysql.cj.result.BooleanValueFactory}
     * </p>
     */
    @Override
    protected boolean convertToBoolean(final int indexBaseZero, final Object sourceValue) {
        final boolean value;

        try {
            if (sourceValue instanceof Boolean) {
                value = (Boolean) sourceValue;
            } else if (sourceValue instanceof Number || sourceValue instanceof String) {
                value = MySQLConvertUtils.convertObjectToBoolean(sourceValue);
            } else if (sourceValue instanceof byte[]) {
                String text = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                value = MySQLConvertUtils.convertObjectToBoolean(text);
            } else {
                throw createNotSupportedException(indexBaseZero, Boolean.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Boolean.class);
        }
    }


    @Override
    protected String convertToString(final int indexBaseZero, final Object sourceValue) {
        final String text;
        if (sourceValue instanceof Long && this.rowMeta.getMySQLType(indexBaseZero) == MySQLType.BIT) {
            text = Long.toBinaryString((Long) sourceValue);
        } else {
            text = super.convertToString(indexBaseZero, sourceValue);
        }
        return text;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    @Override
    protected Duration convertToDuration(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Duration duration;
        if (sourceValue instanceof LocalTime) {
            try {
                //if convert to Duration,must be converted back to ZoneOffset of database.
                LocalTime time = OffsetTime.of((LocalTime) sourceValue, this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalTime();
                duration = MySQLTimeUtils.convertToDuration(time);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, Duration.class);
            }
        } else {
            duration = super.convertToDuration(indexBaseZero, sourceValue);
        }
        return duration;
    }

    @Override
    protected int convertToIndex(String columnAlias) {
        return this.rowMeta.convertToIndex(columnAlias);
    }

    @Override
    protected ZoneOffset obtainZoneOffsetClient() {
        return this.adjutant.obtainZoneOffsetClient();
    }


    @Override
    protected Charset obtainColumnCharset(final int indexBaseZero) {
        return this.adjutant.obtainColumnCharset(this.rowMeta.getColumnCharset(indexBaseZero));
    }

    @Override
    protected TemporalAccessor convertStringToTemporalAccessor(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass)
            throws DateTimeException, UnsupportedConvertingException {

        final TemporalAccessor accessor;

        switch (this.rowMeta.getMySQLType(indexBaseZero)) {
            case DATETIME:
            case TIMESTAMP: {
                LocalDateTime dateTime;
                dateTime = LocalDateTime.parse(sourceValue, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
                accessor = OffsetDateTime.of(dateTime, this.adjutant.obtainZoneOffsetDatabase())
                        .withOffsetSameInstant(obtainZoneOffsetClient())
                        .toLocalDateTime();
            }
            break;
            case DATE:
                accessor = LocalDate.parse(sourceValue);
                break;
            case TIME: {
                LocalTime databaseTime = LocalTime.parse(sourceValue, MySQLTimeUtils.MYSQL_TIME_FORMATTER);
                accessor = OffsetTime.of(databaseTime, this.adjutant.obtainZoneOffsetDatabase())
                        .withOffsetSameInstant(obtainZoneOffsetClient())
                        .toLocalTime();
            }
            break;
            case YEAR:
                accessor = Year.parse(sourceValue);
                break;
            default:
                throw createNotSupportedException(indexBaseZero, targetClass);
        }
        return accessor;
    }

    @Override
    protected TemporalAmount convertStringToTemporalAmount(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        final Duration duration;
        if (this.rowMeta.getMySQLType(indexBaseZero) == MySQLType.TIME) {
            duration = MySQLTimeUtils.parseTimeAsDuration(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, targetClass);
        }
        return duration;
    }

    @Override
    protected String formatTemporalAccessor(final TemporalAccessor temporalAccessor) throws DateTimeException {
        final String text;
        if (temporalAccessor instanceof LocalDateTime) {
            text = ((LocalDateTime) temporalAccessor).format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (temporalAccessor instanceof LocalTime) {
            text = ((LocalTime) temporalAccessor).format(MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        } else if (temporalAccessor instanceof ZonedDateTime) {
            //no bug never here
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .append(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER)
                    .appendOffsetId()
                    .toFormatter(Locale.ENGLISH);

            text = ((ZonedDateTime) temporalAccessor).format(formatter);
        } else if (temporalAccessor instanceof OffsetDateTime) {
            //no bug  never here
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .append(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER)
                    .appendZoneId()
                    .toFormatter(Locale.ENGLISH);

            text = ((OffsetDateTime) temporalAccessor).format(formatter);
        } else if (temporalAccessor instanceof OffsetTime) {
            //no bug  never here
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .append(MySQLTimeUtils.MYSQL_TIME_FORMATTER)
                    .appendOffsetId()
                    .toFormatter(Locale.ENGLISH);

            text = ((OffsetTime) temporalAccessor).format(formatter);
        } else {
            text = temporalAccessor.toString();
        }
        return text;
    }


    @Override
    protected UnsupportedConvertingException createNotSupportedException(final int indexBasedZero
            , final Class<?> targetClass) {
        MySQLType mySQLType = this.rowMeta.getMySQLType(indexBasedZero);

        String message = String.format("Not support convert from (index[%s] alias[%s] and MySQLType[%s]) to %s.",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , mySQLType, targetClass.getName());

        return new UnsupportedConvertingException(message, mySQLType, targetClass);
    }

    @Override
    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause
            , int indexBasedZero, Class<?> targetClass) {
        MySQLType mySQLType = this.rowMeta.getMySQLType(indexBasedZero);

        String f = "Cannot convert value from (index[%s] alias[%s] and MySQLType[%s]) to %s, please check value rang.";
        String m = String.format(f
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , mySQLType, targetClass.getName());

        return new UnsupportedConvertingException(m, cause, mySQLType, targetClass);
    }



    /*################################## blow private method ##################################*/


    private static final class SimpleMySQLResultRow extends MySQLResultRow {

        private SimpleMySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
            super(columnValues, rowMeta, adjutant);
        }
    }
}
