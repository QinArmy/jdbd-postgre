package io.jdbd.vendor.result;

import io.jdbd.JdbdSQLException;
import io.jdbd.ResultRow;
import io.jdbd.vendor.util.JdbdNumberUtils;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public abstract class AbstractResultRow implements ResultRow {

    private final Object[] columnValues;

    protected AbstractResultRow(Object[] columnValues) {
        this.columnValues = columnValues;
    }


    @Nullable
    @Override
    public final Object get(int indexBaseZero) throws JdbdSQLException {
        return this.columnValues[checkIndex(indexBaseZero)];
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public final <T> T get(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        return (value == null || columnClass.isInstance(value))
                ? (T) value
                : convertValue(indexBaseZero, value, columnClass);
    }

    @Nullable
    @Override
    public final Object get(String columnAlias) throws JdbdSQLException {
        try {
            return this.columnValues[checkIndex(convertToIndex(columnAlias))];
        } catch (Throwable e) {
            throw new JdbdSQLException(new SQLException(String.format("alias[%s] access error.", columnAlias), e));
        }
    }

    @Nullable
    @Override
    public final <T> T get(String alias, Class<T> columnClass) throws JdbdSQLException {
        try {
            return get(convertToIndex(alias), columnClass);
        } catch (Throwable e) {
            throw new JdbdSQLException(new SQLException(String.format("alias[%s] access error.", alias), e));
        }
    }


    @Override
    public final Object obtain(int indexBaseZero) throws JdbdSQLException {
        Object value = get(indexBaseZero);
        if (value == null) {
            throw createNotRequiredException(indexBaseZero);
        }
        return value;
    }

    @Override
    public final <T> T obtain(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException {
        T value = get(indexBaseZero, columnClass);
        if (value == null) {
            throw createNotRequiredException(indexBaseZero);
        }
        return value;
    }

    @Override
    public final Object obtain(String columnAlias) throws JdbdSQLException {
        Object value = get(columnAlias);
        if (value == null) {
            throw createNotRequiredException(columnAlias);
        }
        return value;
    }

    @Override
    public final <T> T obtain(String columnAlias, Class<T> columnClass) throws JdbdSQLException {
        T value = get(columnAlias, columnClass);
        if (value == null) {
            throw createNotRequiredException(columnAlias);
        }
        return value;
    }




    /*################################## blow protected template method ##################################*/


    protected abstract JdbdSQLException createNotRequiredException(String columnAlias);

    protected abstract JdbdSQLException createNotRequiredException(int indexBaseZero);

    protected abstract int convertToIndex(String columnAlias);

    protected abstract JdbdSQLException createNotSupportedException(int indexBasedZero, Class<?> valueClass
            , Class<?> targetClass);

    protected abstract JdbdSQLException createValueCannotConvertException(@Nullable Throwable cause, int indexBasedZero
            , Class<?> valueClass, Class<?> targetClass);

    protected abstract ZoneOffset obtainZoneOffsetClient();

    protected abstract ZoneOffset obtainZoneOffsetDatabase();

    protected abstract Charset obtainColumnCharset(int indexBasedZero);

    protected abstract TemporalAccessor convertStringToTemporalAccessor(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass);

    protected abstract boolean isDatabaseSupportTimeZone();

    protected abstract DateTimeFormatter obtainLocalDateTimeFormatter();

    protected abstract DateTimeFormatter obtainLocalTimeFormatter();

    /*################################## blow protected method ##################################*/


    @SuppressWarnings("unchecked")
    protected <T> T convertValue(final int indexBaseZero, final Object nonValue, final Class<T> targetClass) {
        final Object convertedValue;

        if (targetClass == String.class) {
            convertedValue = convertToString(indexBaseZero, nonValue);
        } else if (Number.class.isAssignableFrom(targetClass)) {
            if (targetClass == Integer.class) {
                convertedValue = convertToInteger(indexBaseZero, nonValue);
            } else if (targetClass == Long.class) {
                convertedValue = convertToLong(indexBaseZero, nonValue);
            } else if (targetClass == BigDecimal.class) {
                convertedValue = convertToBigDecimal(indexBaseZero, nonValue);
            } else if (targetClass == BigInteger.class) {
                convertedValue = convertToBigInteger(indexBaseZero, nonValue);
            } else if (targetClass == Byte.class) {
                convertedValue = convertToByte(indexBaseZero, nonValue);
            } else if (targetClass == Short.class) {
                convertedValue = convertToShort(indexBaseZero, nonValue);
            } else if (targetClass == Double.class) {
                convertedValue = convertToDouble(indexBaseZero, nonValue);
            } else if (targetClass == Float.class) {
                convertedValue = convertToFloat(indexBaseZero, nonValue);
            } else {
                throw createNotSupportedException(indexBaseZero, nonValue.getClass(), targetClass);
            }
        } else if (TemporalAccessor.class.isAssignableFrom(targetClass)) {
            if (targetClass == LocalDateTime.class) {
                convertedValue = convertToLocalDateTime(indexBaseZero, nonValue);
            } else if (targetClass == LocalTime.class) {
                convertedValue = convertToLocalTime(indexBaseZero, nonValue);
            } else if (targetClass == LocalDate.class) {
                convertedValue = convertToLocalDate(indexBaseZero, nonValue);
            } else if (targetClass == ZonedDateTime.class) {
                convertedValue = convertToZonedDateTime(indexBaseZero, nonValue);
            } else if (targetClass == OffsetDateTime.class) {
                convertedValue = convertToOffsetDateTime(indexBaseZero, nonValue);
            } else if (targetClass == OffsetTime.class) {
                convertedValue = convertToOffsetTime(indexBaseZero, nonValue);
            } else if (targetClass == Instant.class) {
                convertedValue = convertToInstant(indexBaseZero, nonValue);
            } else if (targetClass == Year.class) {
                convertedValue = convertToYear(indexBaseZero, nonValue);
            } else if (targetClass == YearMonth.class) {
                convertedValue = convertToYearMonth(indexBaseZero, nonValue);
            } else if (targetClass == MonthDay.class) {
                convertedValue = convertToMonthDay(indexBaseZero, nonValue);
            } else if (targetClass == DayOfWeek.class) {
                convertedValue = convertToDayOfWeek(indexBaseZero, nonValue);
            } else if (targetClass == Month.class) {
                convertedValue = convertToMonth(indexBaseZero, nonValue);
            } else {
                throw createNotSupportedException(indexBaseZero, nonValue.getClass(), targetClass);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, nonValue.getClass(), targetClass);
        }
        return (T) convertedValue;
    }


    /**
     * @see #convertValue(int, Object, Class)
     */
    protected OffsetTime convertToOffsetTime(final int indexBaseZero, final Object sourceValue) {
        final OffsetTime newValue;
        if (sourceValue instanceof LocalTime) {
            newValue = OffsetTime.of((LocalTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = OffsetDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient())
                    .toOffsetTime();
        } else if (sourceValue instanceof String) {
            newValue = convertStringToOffsetTime(indexBaseZero, (String) sourceValue, OffsetTime.class);
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetTime(indexBaseZero, textValue, OffsetTime.class);
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toOffsetDateTime()
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toOffsetTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toOffsetTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), OffsetTime.class);
        }
        return newValue;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    protected Instant convertToInstant(final int indexBaseZero, final Object sourceValue) {
        final Instant instant;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    instant = Instant.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    instant = Instant.from(dateTime);
                } else {
                    instant = Instant.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                instant = Instant.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , Instant.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                instant = Instant.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , Instant.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Instant.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Instant.class);
        }
        return instant;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    protected Year convertToYear(final int indexBaseZero, final Object sourceValue) {
        final Year year;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    year = Year.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    year = Year.from(dateTime);
                } else {
                    year = Year.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                year = Year.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , Year.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                year = Year.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , Year.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Year.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Year.class);
        }
        return year;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    protected YearMonth convertToYearMonth(final int indexBaseZero, final Object sourceValue) {
        final YearMonth yearMonth;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    yearMonth = YearMonth.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    yearMonth = YearMonth.from(dateTime);
                } else {
                    yearMonth = YearMonth.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                yearMonth = YearMonth.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , YearMonth.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                yearMonth = YearMonth.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , YearMonth.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), YearMonth.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), YearMonth.class);
        }
        return yearMonth;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    protected MonthDay convertToMonthDay(final int indexBaseZero, final Object sourceValue) {
        final MonthDay monthDay;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    monthDay = MonthDay.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    monthDay = MonthDay.from(dateTime);
                } else {
                    monthDay = MonthDay.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                monthDay = MonthDay.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , MonthDay.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                monthDay = MonthDay.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , MonthDay.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), MonthDay.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), MonthDay.class);
        }
        return monthDay;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    protected DayOfWeek convertToDayOfWeek(final int indexBaseZero, final Object sourceValue) {
        final DayOfWeek dayOfWeek;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    dayOfWeek = DayOfWeek.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    dayOfWeek = DayOfWeek.from(dateTime);
                } else {
                    dayOfWeek = DayOfWeek.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                dayOfWeek = DayOfWeek.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , DayOfWeek.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                dayOfWeek = DayOfWeek.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , DayOfWeek.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), DayOfWeek.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), DayOfWeek.class);
        }
        return dayOfWeek;
    }

    protected Month convertToMonth(final int indexBaseZero, final Object sourceValue) {
        final Month month;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                if (sourceValue instanceof OffsetDateTime) {
                    OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                            .withOffsetSameInstant(obtainZoneOffsetClient());
                    month = Month.from(dateTime);
                } else if (sourceValue instanceof ZonedDateTime) {
                    ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                            .withZoneSameInstant(obtainZoneOffsetClient());
                    month = Month.from(dateTime);
                } else {
                    month = Month.from(((TemporalAccessor) sourceValue));
                }
            } else if (sourceValue instanceof String) {
                month = Month.from(convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                        , Month.class));
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                month = Month.from(convertStringToTemporalAccessor(indexBaseZero, textValue
                        , Month.class));
            } else {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Month.class);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Month.class);
        }
        return month;
    }

    protected OffsetDateTime convertToOffsetDateTime(final int indexBaseZero, final Object sourceValue) {
        final OffsetDateTime newValue;

        if (sourceValue instanceof LocalDateTime) {
            newValue = OffsetDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof String) {
            newValue = convertStringToOffsetDateTime(indexBaseZero, (String) sourceValue, OffsetDateTime.class);
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetDateTime(indexBaseZero, textValue, OffsetDateTime.class);
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient())
                    .toOffsetDateTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), OffsetDateTime.class);
        }
        return newValue;
    }


    protected ZonedDateTime convertToZonedDateTime(final int indexBaseZero, final Object sourceValue) {
        final ZonedDateTime newValue;
        if (sourceValue instanceof LocalDateTime) {
            newValue = ZonedDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof String) {
            newValue = convertStringToOffsetDateTime(indexBaseZero, (String) sourceValue, ZonedDateTime.class)
                    .toZonedDateTime();
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetDateTime(indexBaseZero, textValue, ZonedDateTime.class)
                    .toZonedDateTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toZonedDateTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), ZonedDateTime.class);
        }
        return newValue;
    }


    protected LocalTime convertToLocalTime(final int indexBaseZero, final Object sourceValue) {
        final LocalTime newValue;
        if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalTime();
        } else if (sourceValue instanceof String) {
            newValue = convertStringToOffsetTime(indexBaseZero, (String) sourceValue, LocalTime.class)
                    .toLocalTime();
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetTime(indexBaseZero, textValue, LocalTime.class)
                    .toLocalTime();
        } else if (sourceValue instanceof OffsetTime) {
            newValue = ((OffsetTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalTime.class);
        }
        return newValue;
    }

    protected LocalDateTime convertToLocalDateTime(final int indexBaseZero, final Object sourceValue) {
        final LocalDateTime newValue;
        if (sourceValue instanceof String) {
            newValue = convertStringToOffsetDateTime(indexBaseZero, (String) sourceValue, LocalDateTime.class)
                    .toLocalDateTime();
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetDateTime(indexBaseZero, textValue, LocalDateTime.class)
                    .toLocalDateTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalDateTime.class);
        }
        return newValue;
    }


    protected LocalDate convertToLocalDate(final int indexBaseZero, final Object sourceValue) {
        final LocalDate newValue;
        if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof String) {
            newValue = convertStringToOffsetDateTime(indexBaseZero, (String) sourceValue, LocalDate.class)
                    .toLocalDate();
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            newValue = convertStringToOffsetDateTime(indexBaseZero, textValue, LocalDate.class)
                    .toLocalDate();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalDate();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDate();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalDate.class);
        }
        return newValue;
    }


    protected Double convertToDouble(final int indexBaseZero, final Object sourceValue) {
        final double newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Double.parseDouble((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Double.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Double.parseDouble(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Double.class);
            }
        } else if (sourceValue instanceof Number) {
            if (sourceValue instanceof Integer
                    || sourceValue instanceof Short
                    || sourceValue instanceof Byte
                    || sourceValue instanceof Float) {
                newValue = ((Number) sourceValue).doubleValue();
            } else {
                throw createValueCannotConvertException(null, indexBaseZero, sourceValue.getClass(), Double.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Double.class);
        }
        return newValue;
    }

    protected Float convertToFloat(final int indexBaseZero, final Object sourceValue) {
        final float newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Float.parseFloat((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Float.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Float.parseFloat(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Float.class);
            }
        } else if (sourceValue instanceof Number) {
            if (sourceValue instanceof Short
                    || sourceValue instanceof Byte) {
                newValue = ((Number) sourceValue).floatValue();
            } else {
                throw createValueCannotConvertException(null, indexBaseZero, sourceValue.getClass(), Float.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Float.class);
        }
        return newValue;
    }

    protected BigDecimal convertToBigDecimal(final int indexBaseZero, final Object sourceValue) {
        final BigDecimal newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigDecimal((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigDecimal.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = new BigDecimal(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigDecimal.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToBigDecimal((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigDecimal.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
        }
        return newValue;
    }

    protected BigInteger convertToBigInteger(final int indexBaseZero, final Object sourceValue) {
        final BigInteger newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigInteger((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigInteger.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = new BigInteger(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigInteger.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToBigInteger((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), BigInteger.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigInteger.class);
        }
        return newValue;
    }

    protected Long convertToLong(final int indexBaseZero, final Object sourceValue) {
        final long newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Long.parseLong((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Long.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Long.parseLong(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Long.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToLong((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Long.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Long.class);
        }
        return newValue;
    }

    protected Integer convertToInteger(final int indexBaseZero, final Object sourceValue) {
        final int newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Integer.parseInt((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Integer.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Integer.parseInt(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Integer.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToInt((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Integer.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Integer.class);
        }

        return newValue;

    }

    protected Short convertToShort(final int indexBaseZero, final Object sourceValue) {
        final short newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Short.parseShort((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Short.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Short.parseShort(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Short.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToShort((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Short.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Short.class);
        }
        return newValue;
    }

    protected Byte convertToByte(final int indexBaseZero, Object sourceValue) {
        final byte newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Byte.parseByte((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Byte.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Byte.parseByte(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Byte.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = JdbdNumberUtils.convertNumberToByte((Number) sourceValue);
            } catch (Throwable e) {
                throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), Byte.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Byte.class);
        }
        return newValue;
    }

    protected String convertToString(final int indexBaseZero, final Object sourceValue) {
        final String value;
        if (sourceValue instanceof byte[]) {
            value = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
        } else if (sourceValue instanceof BigDecimal) {
            value = ((BigDecimal) sourceValue).toPlainString();
        } else if (sourceValue instanceof Number) {
            value = sourceValue.toString();
        } else if (sourceValue instanceof TemporalAccessor) {
            if (sourceValue instanceof LocalDateTime) {
                value = ((LocalDateTime) sourceValue).format(obtainLocalDateTimeFormatter());
            } else if (sourceValue instanceof LocalTime) {
                value = ((LocalTime) sourceValue).format(obtainLocalTimeFormatter());
            } else if (sourceValue instanceof OffsetDateTime) {
                value = ((OffsetDateTime) sourceValue)
                        .withOffsetSameInstant(obtainZoneOffsetClient())
                        .toLocalDateTime().format(obtainLocalDateTimeFormatter());
            } else if (sourceValue instanceof ZonedDateTime) {
                value = ((ZonedDateTime) sourceValue)
                        .withZoneSameInstant(obtainZoneOffsetClient())
                        .toLocalDateTime().format(obtainLocalDateTimeFormatter());
            } else if (sourceValue instanceof OffsetTime) {
                value = ((OffsetTime) sourceValue)
                        .withOffsetSameInstant(obtainZoneOffsetClient())
                        .toLocalTime().format(obtainLocalTimeFormatter());
            } else {
                value = sourceValue.toString();
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), String.class);
        }
        return value;
    }


    protected OffsetDateTime convertStringToOffsetDateTime(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass) {
        LocalDateTime dateTime;
        try {
            dateTime = LocalDateTime.parse(sourceValue, obtainLocalDateTimeFormatter());
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), targetClass);
        }
        return OffsetDateTime.of(dateTime, obtainZoneOffsetDatabase())
                .withOffsetSameInstant(obtainZoneOffsetClient());
    }

    protected OffsetTime convertStringToOffsetTime(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass) {
        LocalTime time;
        try {
            time = LocalTime.parse(sourceValue, obtainLocalTimeFormatter());
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), targetClass);
        }
        return OffsetTime.of(time, obtainZoneOffsetDatabase())
                .withOffsetSameInstant(obtainZoneOffsetClient());
    }






    /*################################## blow private method ##################################*/

    private int checkIndex(int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnValues.length) {
            throw new JdbdSQLException(new SQLException(
                    String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnValues.length - 1)));
        }
        return indexBaseZero;
    }


}
