package io.jdbd.vendor.result;

import io.jdbd.JdbdSQLException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.CodeEnum;
import io.jdbd.type.LongBinary;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStrings;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class AbstractResultRow<R extends ResultRowMeta> implements ResultRow {

    protected R rowMeta;

    private final Object[] columnValues;

    protected AbstractResultRow(R rowMeta, Object[] columnValues) {
        if (columnValues.length != rowMeta.getColumnCount()) {
            throw new IllegalArgumentException(String.format("rowMeta columnCount[%s] and columnValues[%s] not match."
                    , rowMeta.getColumnCount(), columnValues.length));
        }
        this.rowMeta = rowMeta;
        this.columnValues = columnValues;
    }

    @Override
    public final int getResultIndex() {
        return this.rowMeta.getResultIndex();
    }

    @Override
    public final ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Nullable
    @Override
    public final Object get(final int indexBaseZero) throws JdbdSQLException {
        final Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        final Object convertedValue;
        final Class<?> javaType = this.rowMeta.getSQLType(indexBaseZero).javaType();
        if (javaType.isAssignableFrom(value.getClass())) {
            convertedValue = value;
        } else {
            convertedValue = convertNonNullValue(indexBaseZero, value, javaType);
        }
        return convertedValue;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public final <T> T get(final int indexBaseZero, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        final Object value = this.columnValues[checkIndex(indexBaseZero)];
        final T convertedValue;
        if (value == null || columnClass == value.getClass()) {
            convertedValue = (T) value;
        } else {
            convertedValue = convertNonNullValue(indexBaseZero, value, columnClass);
        }
        return convertedValue;
    }

    @Nullable
    @Override
    public final Object get(final String columnAlias) throws JdbdSQLException {
        try {
            return this.columnValues[checkIndex(convertToIndex(columnAlias))];
        } catch (Throwable e) {
            throw new JdbdSQLException(new SQLException(String.format("alias[%s] access error.", columnAlias), e));
        }
    }

    @Nullable
    @Override
    public final <T> T get(final String columnAlias, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        try {
            return get(convertToIndex(columnAlias), columnClass);
        } catch (Throwable e) {
            throw new JdbdSQLException(new SQLException(String.format("Column alias[%s] access error.", columnAlias), e));
        }
    }

    @Override
    public final <T> Set<T> getSet(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        final Object value = this.columnValues[checkIndex(indexBaseZero)];
        final Set<T> set;
        if (value == null) {
            set = Collections.emptySet();
        } else {
            set = convertNonNullToSet(indexBaseZero, value, elementClass);
        }
        return set;
    }

    @Override
    public final <T> Set<T> getSet(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        return getSet(convertToIndex(columnAlias), elementClass);
    }

    @Override
    public <T> List<T> getList(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        final Object value = this.columnValues[checkIndex(indexBaseZero)];
        final List<T> list;
        if (value == null) {
            list = Collections.emptyList();
        } else {
            list = convertNonNullToList(indexBaseZero, value, elementClass);
        }
        return list;
    }

    @Override
    public final <T> List<T> getList(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        return getList(convertToIndex(columnAlias), elementClass);
    }

    @Override
    public final Object getNonNull(final int indexBaseZero)
            throws JdbdSQLException, NullPointerException {
        Object value = get(indexBaseZero);
        if (value == null) {
            throw new NullPointerException(String.format("Value at indexBaseZero[%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final <T> T getNonNull(final int indexBaseZero, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException {
        T value = get(indexBaseZero, columnClass);
        if (value == null) {
            throw new NullPointerException(String.format("Value at indexBaseZero[%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final Object getNonNull(final String columnAlias) throws JdbdSQLException, NullPointerException {
        Object value = get(columnAlias);
        if (value == null) {
            throw new NullPointerException(String.format("Value at columnAlias[%s] is null.", columnAlias));
        }
        return value;
    }

    @Override
    public final <T> T getNonNull(final String columnAlias, final Class<T> columnClass)
            throws JdbdSQLException, NullPointerException, UnsupportedConvertingException {
        T value = get(columnAlias, columnClass);
        if (value == null) {
            throw new NullPointerException(String.format("Value at columnAlias[%s] is null.", columnAlias));
        }
        return value;
    }




    /*################################## blow protected template method ##################################*/


    protected abstract int convertToIndex(String columnAlias);

    protected abstract UnsupportedConvertingException createNotSupportedException(int indexBasedZero
            , Class<?> targetClass);

    protected abstract UnsupportedConvertingException createValueCannotConvertException(Throwable cause
            , int indexBasedZero, Class<?> targetClass);

    protected abstract ZoneOffset obtainZoneOffsetClient();


    protected abstract Charset obtainColumnCharset(int indexBasedZero);

    protected abstract TemporalAccessor convertStringToTemporalAccessor(final int indexBaseZero
            , final String sourceValue, Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException;

    protected abstract TemporalAmount convertStringToTemporalAmount(final int indexBaseZero, final String sourceValue
            , Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException;

    protected abstract String formatTemporalAccessor(TemporalAccessor temporalAccessor) throws DateTimeException;

    protected abstract boolean convertToBoolean(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException;

    /*################################## blow protected method ##################################*/


    @SuppressWarnings("unchecked")
    private <T> T convertNonNullValue(final int indexBaseZero, final Object nonValue, final Class<T> targetClass)
            throws UnsupportedConvertingException {
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
                convertedValue = convertToOtherNumber(indexBaseZero, nonValue, (Class<? extends Number>) targetClass);
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
                convertedValue = convertToOtherTemporalAccessor(indexBaseZero, nonValue
                        , (Class<? extends TemporalAccessor>) targetClass);
            }
        } else if (TemporalAmount.class.isAssignableFrom(targetClass)) {
            if (targetClass == Duration.class) {
                convertedValue = convertToDuration(indexBaseZero, nonValue);
            } else if (targetClass == Period.class) {
                convertedValue = convertToPeriod(indexBaseZero, nonValue);
            } else {
                convertedValue = convertToOtherTemporalAmount(indexBaseZero, nonValue
                        , (Class<? extends TemporalAmount>) targetClass);
            }
        } else if (targetClass == Boolean.class) {
            convertedValue = convertToBoolean(indexBaseZero, nonValue);
        } else if (targetClass == byte[].class) {
            convertedValue = convertToByteArray(indexBaseZero, nonValue);
        } else if (targetClass.isEnum()) {
            final Enum<?> enumValue;
            enumValue = convertToEnum(indexBaseZero, nonValue, targetClass);
            convertedValue = enumValue;
        } else {
            convertedValue = convertToOther(indexBaseZero, nonValue, targetClass);
        }
        return (T) convertedValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    @SuppressWarnings("unchecked")
    protected <T> Set<T> convertNonNullToSet(final int indexBaseZero, final Object nonValue
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        try {
            final Set<T> set;
            if (nonValue instanceof String) {
                Set<String> stringSet = JdbdStrings.spitAsSet((String) nonValue, ",");
                if (elementClass.isEnum()) {
                    Set<T> tempSet = JdbdStrings.convertStringsToEnumSet(stringSet, elementClass);
                    set = JdbdCollections.unmodifiableSet(tempSet);
                } else if (elementClass == String.class) {
                    set = (Set<T>) JdbdCollections.unmodifiableSet(stringSet);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
            return set;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Set.class);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    @SuppressWarnings("unchecked")
    protected <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonValue
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        try {
            final List<T> list;
            if (nonValue instanceof String) {
                List<String> stringList = JdbdStrings.spitAsList((String) nonValue, ",");
                if (elementClass.isEnum()) {
                    List<T> tempList = JdbdStrings.convertStringsToEnumList(stringList, elementClass);
                    list = JdbdCollections.unmodifiableList(tempList);
                } else if (elementClass == String.class) {
                    list = (List<T>) JdbdCollections.unmodifiableList(stringList);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
            return list;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Set.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected <T> T convertToOther(final int indexBaseZero, final Object sourceValue
            , final Class<T> targetClass) throws UnsupportedConvertingException {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Number convertToOtherNumber(final int indexBaseZero, final Object sourceValue
            , final Class<? extends Number> targetClass) {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected TemporalAccessor convertToOtherTemporalAccessor(final int indexBaseZero, final Object sourceValue
            , final Class<? extends TemporalAccessor> targetClass) {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected TemporalAmount convertToOtherTemporalAmount(final int indexBaseZero, final Object sourceValue
            , final Class<? extends TemporalAmount> targetClass) {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    @SuppressWarnings("unchecked")
    protected <T extends Enum<T>> T convertToEnum(final int indexBaseZero, final Object sourceValue
            , final Class<?> enumClass) throws UnsupportedConvertingException {
        final T enumValue;
        try {
            final Class<T> clazz = (Class<T>) enumClass;
            if (sourceValue instanceof Integer && CodeEnum.class.isAssignableFrom(enumClass)) {
                enumValue = (T) CodeEnum.resolve(enumClass, (Integer) sourceValue);
            } else if (sourceValue instanceof String) {

                enumValue = Enum.valueOf(clazz, (String) sourceValue);
            } else if (sourceValue instanceof byte[]) {
                String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                enumValue = Enum.valueOf(clazz, textValue);
            } else {
                throw createNotSupportedException(indexBaseZero, enumClass);
            }
            return enumValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, enumClass);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected byte[] convertToByteArray(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final byte[] value;

        if (sourceValue instanceof byte[]) {
            value = (byte[]) sourceValue;
        } else if (sourceValue instanceof String) {
            value = ((String) sourceValue).getBytes(obtainColumnCharset(indexBaseZero));
        } else if (sourceValue instanceof LongBinary) {
            LongBinary longBinary = (LongBinary) sourceValue;
            if (longBinary.isArray()) {
                value = longBinary.asArray();
            } else {
                throw createNotSupportedException(indexBaseZero, byte[].class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, byte[].class);
        }
        return value;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected String convertToString(final int indexBaseZero, final Object sourceValue) {
        final String value;
        try {
            if (sourceValue instanceof String) {
                value = (String) sourceValue;
            } else if (sourceValue instanceof byte[]) {
                value = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            } else if (sourceValue instanceof BigDecimal) {
                value = ((BigDecimal) sourceValue).toPlainString();
            } else if (sourceValue instanceof Number) {
                value = sourceValue.toString();
            } else if (sourceValue instanceof LongString) {
                LongString longString = (LongString) sourceValue;
                if (longString.isString()) {
                    value = longString.asString();
                } else {
                    throw createNotSupportedException(indexBaseZero, String.class);
                }
            } else if (sourceValue instanceof TemporalAccessor) {
                value = formatTemporalAccessor((TemporalAccessor) sourceValue);
            } else {
                throw createNotSupportedException(indexBaseZero, String.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, String.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Duration convertToDuration(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Duration value;

        try {
            if (sourceValue instanceof TemporalAmount) {
                value = Duration.from((TemporalAmount) sourceValue);
            } else {
                final TemporalAmount amount;
                if (sourceValue instanceof String) {
                    amount = convertStringToTemporalAmount(indexBaseZero, (String) sourceValue, Duration.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    amount = convertStringToTemporalAmount(indexBaseZero, textValue, Duration.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, Duration.class);
                }
                value = Duration.from(amount);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Duration.class);
        }


    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Period convertToPeriod(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Period value;

        try {
            if (sourceValue instanceof TemporalAmount) {
                value = Period.from((TemporalAmount) sourceValue);
            } else {
                final TemporalAmount amount;
                if (sourceValue instanceof String) {
                    amount = convertStringToTemporalAmount(indexBaseZero, (String) sourceValue, Period.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    amount = convertStringToTemporalAmount(indexBaseZero, textValue, Period.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, Period.class);
                }
                value = Period.from(amount);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Period.class);
        }


    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected OffsetTime convertToOffsetTime(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final OffsetTime newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToOffsetTime((TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, OffsetTime.class);

                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, OffsetTime.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, OffsetTime.class);
                }

                newValue = convertTemporalAccessorToOffsetTime(accessor);
            }
            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, OffsetTime.class);
        }
    }

    /**
     * @see #convertToOffsetTime(int, Object)
     */
    protected OffsetTime convertTemporalAccessorToOffsetTime(final TemporalAccessor sourceValue)
            throws DateTimeException {

        final OffsetTime newValue;

        if (sourceValue instanceof OffsetTime) {
            newValue = (OffsetTime) sourceValue;
        } else if (sourceValue instanceof LocalTime) {
            newValue = OffsetTime.of((LocalTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = OffsetDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient())
                    .toOffsetTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue)
                    .toOffsetDateTime()
                    .toOffsetTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .toOffsetTime();
        } else if (sourceValue instanceof Instant) {
            newValue = OffsetDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC)
                    .toOffsetTime();
        } else {
            newValue = OffsetTime.from(sourceValue);
        }
        return newValue;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Instant convertToInstant(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Instant instant;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                instant = convertTemporalToInstant(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, Instant.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, Instant.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, Instant.class);
                }

                instant = convertTemporalToInstant(indexBaseZero, accessor);

            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Instant.class);
        }
        return instant;
    }

    /**
     * @see #convertToInstant(int, Object)
     */
    protected Instant convertTemporalToInstant(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {

        final Instant instant;
        if (sourceValue instanceof Instant) {
            instant = (Instant) sourceValue;
        } else if (sourceValue instanceof LocalDateTime) {
            instant = OffsetDateTime.of(((LocalDateTime) sourceValue), obtainZoneOffsetClient())
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else if (sourceValue instanceof ZonedDateTime) {
            instant = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else if (sourceValue instanceof OffsetDateTime) {
            instant = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else {
            throw createNotSupportedException(indexBaseZero, Instant.class);
        }
        return instant;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Year convertToYear(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Year year;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                year = convertTemporalAccessorToYear(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, Year.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, Year.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, Year.class);
                }

                year = convertTemporalAccessorToYear(indexBaseZero, accessor);
            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Year.class);
        }
        return year;
    }

    /**
     * @see #convertToYear(int, Object)
     */
    protected Year convertTemporalAccessorToYear(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {
        final Year year;
        if (sourceValue instanceof Year) {
            year = (Year) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient());
            year = Year.from(dateTime);
        } else if (sourceValue instanceof ZonedDateTime) {
            ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient());
            year = Year.from(dateTime);
        } else if (sourceValue instanceof LocalDateTime
                || sourceValue instanceof LocalDate
                || sourceValue instanceof YearMonth) {
            year = Year.from(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, Year.class);
        }
        return year;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected YearMonth convertToYearMonth(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final YearMonth yearMonth;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                yearMonth = convertTemporalAccessorToYearMonth(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, YearMonth.class);

                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, YearMonth.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, YearMonth.class);
                }
                yearMonth = convertTemporalAccessorToYearMonth(indexBaseZero, accessor);
            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, YearMonth.class);
        }
        return yearMonth;
    }

    /**
     * @see #convertToYearMonth(int, Object)
     */
    protected YearMonth convertTemporalAccessorToYearMonth(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {
        final YearMonth yearMonth;

        if (sourceValue instanceof YearMonth) {
            yearMonth = (YearMonth) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient());
            yearMonth = YearMonth.from(dateTime);
        } else if (sourceValue instanceof ZonedDateTime) {
            ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient());
            yearMonth = YearMonth.from(dateTime);
        } else if (sourceValue instanceof LocalDateTime
                || sourceValue instanceof LocalDate) {
            yearMonth = YearMonth.from(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, YearMonth.class);
        }
        return yearMonth;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected MonthDay convertToMonthDay(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final MonthDay monthDay;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                monthDay = convertTemporalAccessorToMonthDay(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;

                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, MonthDay.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, MonthDay.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, MonthDay.class);
                }

                monthDay = convertTemporalAccessorToMonthDay(indexBaseZero, accessor);

            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, MonthDay.class);
        }
        return monthDay;
    }

    /**
     * @see #convertToMonthDay(int, Object)
     */
    protected MonthDay convertTemporalAccessorToMonthDay(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {
        final MonthDay monthDay;

        if (sourceValue instanceof MonthDay) {
            monthDay = (MonthDay) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient());
            monthDay = MonthDay.from(dateTime);
        } else if (sourceValue instanceof ZonedDateTime) {
            ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient());
            monthDay = MonthDay.from(dateTime);
        } else if (sourceValue instanceof LocalDateTime
                || sourceValue instanceof LocalDate) {
            monthDay = MonthDay.from(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, MonthDay.class);
        }
        return monthDay;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected DayOfWeek convertToDayOfWeek(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {

        final DayOfWeek dayOfWeek;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                dayOfWeek = convertTemporalAccessorToDayOfWeek(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;

                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, DayOfWeek.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, DayOfWeek.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, DayOfWeek.class);
                }

                dayOfWeek = convertTemporalAccessorToDayOfWeek(indexBaseZero, accessor);

            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, DayOfWeek.class);
        }
        return dayOfWeek;
    }

    /**
     * @see #convertToDayOfWeek(int, Object)
     */
    protected DayOfWeek convertTemporalAccessorToDayOfWeek(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {
        final DayOfWeek dayOfWeek;
        if (sourceValue instanceof DayOfWeek) {
            dayOfWeek = (DayOfWeek) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient());
            dayOfWeek = DayOfWeek.from(dateTime);
        } else if (sourceValue instanceof ZonedDateTime) {
            ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient());
            dayOfWeek = DayOfWeek.from(dateTime);
        } else if (sourceValue instanceof LocalDateTime
                || sourceValue instanceof LocalDate) {
            dayOfWeek = DayOfWeek.from(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, DayOfWeek.class);
        }
        return dayOfWeek;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Month convertToMonth(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final Month month;
        try {
            if (sourceValue instanceof TemporalAccessor) {
                month = convertTemporalAccessorToMonth(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, Month.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, Month.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, Month.class);
                }

                month = convertTemporalAccessorToMonth(indexBaseZero, accessor);

            }
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Month.class);
        }
        return month;
    }


    /**
     * @see #convertToMonth(int, Object)
     */
    protected Month convertTemporalAccessorToMonth(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws DateTimeException {
        final Month month;
        if (sourceValue instanceof Month) {
            month = (Month) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            OffsetDateTime dateTime = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(obtainZoneOffsetClient());
            month = Month.from(dateTime);
        } else if (sourceValue instanceof ZonedDateTime) {
            ZonedDateTime dateTime = ((ZonedDateTime) sourceValue)
                    .withZoneSameInstant(obtainZoneOffsetClient());
            month = Month.from(dateTime);
        } else if (sourceValue instanceof LocalDateTime
                || sourceValue instanceof LocalDate
                || sourceValue instanceof YearMonth
                || sourceValue instanceof MonthDay) {
            month = Month.from(sourceValue);
        } else {
            throw createNotSupportedException(indexBaseZero, Month.class);
        }
        return month;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected OffsetDateTime convertToOffsetDateTime(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final OffsetDateTime newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToOffsetDateTime((TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                            , OffsetDateTime.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue
                            , OffsetDateTime.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, OffsetDateTime.class);
                }

                newValue = convertTemporalAccessorToOffsetDateTime(accessor);

            }
            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, OffsetDateTime.class);
        }

    }

    /**
     * @see #convertToOffsetDateTime(int, Object)
     */
    protected OffsetDateTime convertTemporalAccessorToOffsetDateTime(final TemporalAccessor sourceValue)
            throws DateTimeException {
        final OffsetDateTime newValue;
        if (sourceValue instanceof OffsetDateTime) {
            newValue = (OffsetDateTime) sourceValue;
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = OffsetDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toOffsetDateTime();
        } else if (sourceValue instanceof Instant) {
            newValue = OffsetDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC);
        } else {
            newValue = OffsetDateTime.from(sourceValue);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected ZonedDateTime convertToZonedDateTime(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final ZonedDateTime newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToZonedDateTime((TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                            , ZonedDateTime.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, ZonedDateTime.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, ZonedDateTime.class);
                }

                newValue = convertTemporalAccessorToZonedDateTime(accessor);
            }
            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, ZonedDateTime.class);
        }
    }

    /**
     * @see #convertToZonedDateTime(int, Object)
     */
    protected ZonedDateTime convertTemporalAccessorToZonedDateTime(final TemporalAccessor sourceValue)
            throws DateTimeException {
        final ZonedDateTime newValue;
        if (sourceValue instanceof ZonedDateTime) {
            newValue = (ZonedDateTime) sourceValue;
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = ZonedDateTime.of((LocalDateTime) sourceValue, obtainZoneOffsetClient());
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toZonedDateTime();
        } else if (sourceValue instanceof Instant) {
            newValue = ZonedDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC);
        } else {
            newValue = ZonedDateTime.from(sourceValue);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected LocalTime convertToLocalTime(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final LocalTime newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToLocalTime(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, LocalTime.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, LocalTime.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, LocalTime.class);
                }

                newValue = convertTemporalAccessorToLocalTime(indexBaseZero, accessor);

            }
            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalTime.class);
        }
    }

    /**
     * @see #convertToLocalTime(int, Object)
     */
    protected LocalTime convertTemporalAccessorToLocalTime(final int indexBaseZero, final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {
        final LocalTime newValue;

        if (sourceValue instanceof LocalTime) {
            newValue = (LocalTime) sourceValue;
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalTime();
        } else if (sourceValue instanceof OffsetTime) {
            newValue = ((OffsetTime) sourceValue).withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else if (sourceValue instanceof Instant) {
            newValue = OffsetDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalTime();
        } else {
            throw createNotSupportedException(indexBaseZero, LocalTime.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected LocalDateTime convertToLocalDateTime(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final LocalDateTime newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToLocalDateTime(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;
                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue
                            , LocalDateTime.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, LocalDateTime.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, LocalDateTime.class);
                }

                newValue = convertTemporalAccessorToLocalDateTime(indexBaseZero, accessor);

            }
            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalDateTime.class);
        }
    }

    /**
     * @see #convertToLocalDateTime(int, Object)
     */
    protected LocalDateTime convertTemporalAccessorToLocalDateTime(final int indexBaseZero
            , final TemporalAccessor sourceValue) throws UnsupportedConvertingException {

        final LocalDateTime newValue;

        if (sourceValue instanceof LocalDateTime) {
            newValue = (LocalDateTime) sourceValue;
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else if (sourceValue instanceof Instant) {
            newValue = OffsetDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDateTime();
        } else {
            throw createNotSupportedException(indexBaseZero, LocalDateTime.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected LocalDate convertToLocalDate(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final LocalDate newValue;

        try {
            if (sourceValue instanceof TemporalAccessor) {
                newValue = convertTemporalAccessorToLocalDate(indexBaseZero, (TemporalAccessor) sourceValue);
            } else {
                final TemporalAccessor accessor;

                if (sourceValue instanceof String) {
                    accessor = convertStringToTemporalAccessor(indexBaseZero, (String) sourceValue, LocalDate.class);
                } else if (sourceValue instanceof byte[]) {
                    String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
                    accessor = convertStringToTemporalAccessor(indexBaseZero, textValue, LocalDate.class);
                } else {
                    throw createNotSupportedException(indexBaseZero, LocalDate.class);
                }

                newValue = convertTemporalAccessorToLocalDate(indexBaseZero, accessor);
            }

            return newValue;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalDate.class);
        }

    }

    /**
     * @see #convertToLocalDate(int, Object)
     */
    protected LocalDate convertTemporalAccessorToLocalDate(final int indexBaseZero
            , final TemporalAccessor sourceValue)
            throws UnsupportedConvertingException {

        final LocalDate newValue;
        if (sourceValue instanceof LocalDate) {
            newValue = (LocalDate) sourceValue;
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDate();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).withZoneSameInstant(obtainZoneOffsetClient())
                    .toLocalDate();
        } else if (sourceValue instanceof Instant) {
            newValue = OffsetDateTime.ofInstant((Instant) sourceValue, ZoneOffset.UTC)
                    .withOffsetSameInstant(obtainZoneOffsetClient())
                    .toLocalDate();
        } else {
            throw createNotSupportedException(indexBaseZero, LocalDate.class);
        }
        return newValue;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Double convertToDouble(final int indexBaseZero, final Object sourceValue) {
        final double newValue;
        if (sourceValue instanceof Double
                || sourceValue instanceof Float) {
            newValue = ((Number) sourceValue).doubleValue();
        } else if (sourceValue instanceof String) {
            try {
                newValue = Double.parseDouble((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Double.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Double.parseDouble(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Double.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? 1.0D : 0.0D;
        } else {
            throw createNotSupportedException(indexBaseZero, Double.class);
        }
        return newValue;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Float convertToFloat(final int indexBaseZero, final Object sourceValue) {
        final float newValue;
        if (sourceValue instanceof Float) {
            newValue = (Float) sourceValue;
        } else if (sourceValue instanceof String) {
            try {
                newValue = Float.parseFloat((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Float.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Float.parseFloat(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Float.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? 1.0F : 0.0F;
        } else {
            throw createNotSupportedException(indexBaseZero, Float.class);
        }
        return newValue;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected BigDecimal convertToBigDecimal(final int indexBaseZero, final Object sourceValue) {
        final BigDecimal newValue;

        if (sourceValue instanceof Number) {
            if (sourceValue instanceof BigInteger) {
                newValue = new BigDecimal((BigInteger) sourceValue);
            } else if (sourceValue instanceof Integer
                    || sourceValue instanceof Long
                    || sourceValue instanceof Short
                    || sourceValue instanceof Byte) {
                newValue = BigDecimal.valueOf(((Number) sourceValue).longValue());
            } else if (sourceValue instanceof Double
                    || sourceValue instanceof Float) {
                newValue = BigDecimal.valueOf(((Number) sourceValue).doubleValue());
            } else {
                throw createNotSupportedException(indexBaseZero, BigDecimal.class);
            }
        } else if (sourceValue instanceof String) {
            try {
                newValue = new BigDecimal((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigDecimal.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = new BigDecimal(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigDecimal.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? BigDecimal.ONE : BigDecimal.ZERO;
        } else {
            throw createNotSupportedException(indexBaseZero, BigDecimal.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected BigInteger convertToBigInteger(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final BigInteger newValue;

        if (sourceValue instanceof Number) {
            if (sourceValue instanceof BigInteger) {
                newValue = (BigInteger) sourceValue;
            } else if (sourceValue instanceof BigDecimal) {
                newValue = ((BigDecimal) sourceValue).toBigInteger();
            } else if (sourceValue instanceof Integer
                    || sourceValue instanceof Long
                    || sourceValue instanceof Short
                    || sourceValue instanceof Byte) {
                newValue = BigInteger.valueOf(((Number) sourceValue).longValue());
            } else {
                throw createNotSupportedException(indexBaseZero, BigInteger.class);
            }
        } else if (sourceValue instanceof String) {
            try {
                newValue = new BigInteger((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigInteger.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = new BigInteger(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigInteger.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? BigInteger.ONE : BigInteger.ZERO;
        } else {
            throw createNotSupportedException(indexBaseZero, BigInteger.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Long convertToLong(final int indexBaseZero, final Object sourceValue) {
        final long newValue;

        if (sourceValue instanceof Number) {
            newValue = ((Number) sourceValue).longValue();
        } else if (sourceValue instanceof String) {
            try {
                newValue = Long.parseLong((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Long.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Long.parseLong(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Long.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? 1L : 0L;
        } else {
            throw createNotSupportedException(indexBaseZero, Long.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Integer convertToInteger(final int indexBaseZero, final Object sourceValue) {
        final int newValue;

        if (sourceValue instanceof Number) {
            newValue = ((Number) sourceValue).intValue();
        } else if (sourceValue instanceof String) {
            try {
                newValue = Integer.parseInt((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Integer.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Integer.parseInt(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Integer.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? 1 : 0;
        } else {
            throw createNotSupportedException(indexBaseZero, Integer.class);
        }

        return newValue;

    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Short convertToShort(final int indexBaseZero, final Object sourceValue) {
        final short newValue;

        if (sourceValue instanceof Number) {
            newValue = ((Number) sourceValue).shortValue();
        } else if (sourceValue instanceof String) {
            try {
                newValue = Short.parseShort((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Short.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Short.parseShort(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Short.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? (short) 1 : (short) 0;
        } else {
            throw createNotSupportedException(indexBaseZero, Short.class);
        }
        return newValue;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Byte convertToByte(final int indexBaseZero, final Object sourceValue) {
        final byte newValue;
        if (sourceValue instanceof Number) {
            newValue = ((Number) sourceValue).byteValue();
        } else if (sourceValue instanceof String) {
            try {
                newValue = Byte.parseByte((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Byte.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = Byte.parseByte(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Byte.class);
            }
        } else if (sourceValue instanceof Boolean) {
            newValue = ((Boolean) sourceValue) ? (byte) 1 : (byte) 0;
        } else {
            throw createNotSupportedException(indexBaseZero, Byte.class);
        }
        return newValue;
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
