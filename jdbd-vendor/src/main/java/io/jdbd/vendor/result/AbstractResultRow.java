package io.jdbd.vendor.result;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.CodeEnum;
import io.jdbd.type.Interval;
import io.jdbd.type.LongBinary;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdStrings;
import io.jdbd.vendor.util.JdbdTimes;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.*;

public abstract class AbstractResultRow<R extends ResultRowMeta> implements ResultRow {

    protected final R rowMeta;

    protected final Object[] columnValues;

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
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        if (needParse(indexBaseZero, null)) {
            value = parseColumn(indexBaseZero, value, null);
        }
        final Class<?> javaType = this.rowMeta.getSQLType(indexBaseZero).outputJavaType();
        if (!javaType.isInstance(value)) {
            value = convertNonNullValue(indexBaseZero, value, javaType);
        }
        return value;
    }

    @Nullable
    @Override
    public final <T> T get(final int indexBaseZero, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        if (needParse(indexBaseZero, columnClass)) {
            value = parseColumn(indexBaseZero, value, columnClass);
        }
        final T v;
        if (columnClass.isInstance(value)) {
            v = columnClass.cast(value);
        } else {
            v = convertNonNullValue(indexBaseZero, value, columnClass);
        }
        return v;
    }

    @Nullable
    @Override
    public final Object get(final String columnLabel) throws JdbdSQLException {
        return get(this.rowMeta.getColumnIndex(columnLabel));
    }

    @Nullable
    @Override
    public final <T> T get(final String columnLabel, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        return get(this.rowMeta.getColumnIndex(columnLabel), columnClass);
    }


    @Override
    public final Object getNonNull(final int indexBaseZero) throws NullPointerException {
        final Object value = get(indexBaseZero);
        if (value == null) {
            throw new NullPointerException(String.format("Value at indexBaseZero[%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final <T> T getNonNull(final int indexBaseZero, final Class<T> columnClass) throws NullPointerException {
        T value = get(indexBaseZero, columnClass);
        if (value == null) {
            throw new NullPointerException(String.format("Value at indexBaseZero[%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final Object getNonNull(final String columnLabel) {
        final Object value = get(columnLabel);
        if (value == null) {
            throw new NullPointerException(String.format("Value at columnLabel[%s] is null.", columnLabel));
        }
        return value;
    }

    @Override
    public final <T> T getNonNull(final String columnLabel, final Class<T> columnClass) throws NullPointerException {
        final T value = get(columnLabel, columnClass);
        if (value == null) {
            throw new NullPointerException(String.format("Value at columnLabel[%s] is null.", columnLabel));
        }
        return value;
    }

    @Override
    public final <T> List<T> getList(final int indexBaseZero, final Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return Collections.emptyList();
        }
        if (needParse(indexBaseZero, List.class)) {
            value = parseColumn(indexBaseZero, value, List.class);
        }
        return convertNonNullToList(indexBaseZero, value, elementClass);
    }

    @Override
    public final <T> List<T> getList(final String columnLabel, final Class<T> elementClass) {
        return getList(this.rowMeta.getColumnIndex(columnLabel), elementClass);
    }

    @Override
    public final <T> Set<T> getSet(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return Collections.emptySet();
        }
        if (needParse(indexBaseZero, Set.class)) {
            value = parseColumn(indexBaseZero, value, Set.class);
        }
        return convertNonNullToSet(indexBaseZero, value, elementClass);
    }

    @Override
    public final <T> Set<T> getSet(final String columnLabel, final Class<T> elementClass) {
        return getSet(this.rowMeta.getColumnIndex(columnLabel), elementClass);
    }


    @Override
    public final <K, V> Map<K, V> getMap(final int indexBaseZero, final Class<K> keyClass, final Class<V> valueClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return Collections.emptyMap();
        }
        if (needParse(indexBaseZero, Map.class)) {
            value = parseColumn(indexBaseZero, value, Map.class);
        }
        return convertNonNullToMap(indexBaseZero, value, keyClass, valueClass);
    }

    @Override
    public final <K, V> Map<K, V> getMap(String columnLabel, Class<K> keyClass, Class<V> valueClass) {
        return getMap(this.rowMeta.getColumnIndex(columnLabel), keyClass, valueClass);
    }


    @Override
    public <T> Publisher<T> getPublisher(int indexBaseZero, Class<T> valueClass) {
        throw createNotSupportedException(indexBaseZero, valueClass);
    }

    @Override
    public final <T> Publisher<T> getPublisher(String columnLabel, Class<T> valueClass) {
        return getPublisher(this.rowMeta.getColumnIndex(columnLabel), valueClass);
    }


    /*################################## blow protected template method ##################################*/

    protected abstract boolean needParse(final int indexBaseZero, @Nullable final Class<?> columnClass);


    protected abstract Object parseColumn(final int indexBaseZero, Object nonNull, @Nullable final Class<?> columnClass);

    protected abstract Charset obtainColumnCharset(int indexBasedZero);



    /*################################## blow protected method ##################################*/

    protected final <T> T convertNonNullValue(final int indexBaseZero, final Object nonNull, final Class<T> columnClass)
            throws UnsupportedConvertingException {
        final Object value;

        if (columnClass == String.class) {
            value = convertToString(indexBaseZero, nonNull);
        } else if (Number.class.isAssignableFrom(columnClass)) {
            if (columnClass == Integer.class) {
                value = convertToInteger(indexBaseZero, nonNull);
            } else if (columnClass == Long.class) {
                value = convertToLong(indexBaseZero, nonNull);
            } else if (columnClass == BigDecimal.class) {
                value = convertToBigDecimal(indexBaseZero, nonNull);
            } else if (columnClass == BigInteger.class) {
                value = convertToBigInteger(indexBaseZero, nonNull);
            } else if (columnClass == Byte.class) {
                value = convertToByte(indexBaseZero, nonNull);
            } else if (columnClass == Short.class) {
                value = convertToShort(indexBaseZero, nonNull);
            } else if (columnClass == Double.class) {
                value = convertToDouble(indexBaseZero, nonNull);
            } else if (columnClass == Float.class) {
                value = convertToFloat(indexBaseZero, nonNull);
            } else {
                throw createNotSupportedException(indexBaseZero, columnClass);
            }
        } else if (TemporalAccessor.class.isAssignableFrom(columnClass)) {
            if (columnClass == LocalDateTime.class) {
                value = convertToLocalDateTime(indexBaseZero, nonNull);
            } else if (columnClass == LocalTime.class) {
                value = convertToLocalTime(indexBaseZero, nonNull);
            } else if (columnClass == LocalDate.class) {
                value = convertToLocalDate(indexBaseZero, nonNull);
            } else if (columnClass == ZonedDateTime.class) {
                value = convertToZonedDateTime(indexBaseZero, nonNull);
            } else if (columnClass == OffsetDateTime.class) {
                value = convertToOffsetDateTime(indexBaseZero, nonNull);
            } else if (columnClass == OffsetTime.class) {
                value = convertToOffsetTime(indexBaseZero, nonNull);
            } else if (columnClass == Instant.class) {
                value = convertToInstant(indexBaseZero, nonNull);
            } else if (columnClass == Year.class) {
                value = convertToYear(indexBaseZero, nonNull);
            } else if (columnClass == YearMonth.class) {
                value = convertToYearMonth(indexBaseZero, nonNull);
            } else if (columnClass == MonthDay.class) {
                value = convertToMonthDay(indexBaseZero, nonNull);
            } else {
                throw createNotSupportedException(indexBaseZero, columnClass);
            }
        } else if (TemporalAmount.class.isAssignableFrom(columnClass)) {
            if (columnClass == Duration.class) {
                value = convertToDuration(indexBaseZero, nonNull);
            } else if (columnClass == Period.class) {
                value = convertToPeriod(indexBaseZero, nonNull);
            } else if (columnClass == Interval.class) {
                value = convertToInterval(indexBaseZero, nonNull);
            } else {
                throw createNotSupportedException(indexBaseZero, columnClass);
            }
        } else if (columnClass == Boolean.class) {
            value = convertNonNullToBoolean(indexBaseZero, nonNull);
        } else if (columnClass == byte[].class) {
            value = convertToByteArray(indexBaseZero, nonNull);
        } else if (columnClass.isEnum()) {
            final Enum<?> enumValue;
            enumValue = convertToEnum(indexBaseZero, nonNull, columnClass);
            value = enumValue;
        } else if (columnClass.isArray()) {
            value = convertNonNullToArray(indexBaseZero, nonNull, columnClass);
        } else {
            value = convertToOther(indexBaseZero, nonNull, columnClass);
        }
        return columnClass.cast(value);
    }


    protected final boolean convertNonNullToBoolean(final int indexBaseZero, final Object sourceValue)
            throws UnsupportedConvertingException {
        final boolean value;
        if (sourceValue instanceof Boolean) {
            value = (Boolean) sourceValue;
        } else if (sourceValue instanceof String) {
            final String v = (String) sourceValue;
            if (v.equalsIgnoreCase("TRUE")
                    || v.equalsIgnoreCase("T")) {
                value = true;
            } else if (v.equalsIgnoreCase("FALSE")
                    || v.equalsIgnoreCase("F")) {
                value = false;
            } else {
                throw createNotSupportedException(indexBaseZero, Boolean.class);
            }
        } else if (sourceValue instanceof Integer
                || sourceValue instanceof Long
                || sourceValue instanceof Short
                || sourceValue instanceof Byte) {
            value = ((Number) sourceValue).longValue() != 0;
        } else if (sourceValue instanceof Double
                || sourceValue instanceof Float) {
            value = ((Number) sourceValue).doubleValue() != 0.0;
        } else if (sourceValue instanceof BigDecimal) {
            value = ((BigDecimal) sourceValue).compareTo(BigDecimal.ZERO) != 0;
        } else if (sourceValue instanceof BigInteger) {
            value = ((BigInteger) sourceValue).compareTo(BigInteger.ZERO) != 0;
        } else {
            throw createNotSupportedException(indexBaseZero, Boolean.class);
        }
        return value;
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
     * @param nonNull The return value of {@link #parseColumn(int, Object, Class)}.
     * @see #convertNonNullValue(int, Object, Class)
     */
    @SuppressWarnings("unchecked")
    protected <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonNull
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        try {
            final List<T> list;
            if (nonNull instanceof String) {
                List<String> stringList = JdbdStrings.spitAsList((String) nonNull, ",");
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

    protected <K, V> Map<K, V> convertNonNullToMap(final int indexBaseZero, final Object nonNull
            , final Class<K> keyClass, final Class<V> valueClass) {
        throw createNotSupportedException(indexBaseZero, Map.class);
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected Object convertNonNullToArray(final int indexBaseZero, final Object nonNull, final Class<?> targetClass)
            throws UnsupportedConvertingException {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected <T> T convertToOther(final int indexBaseZero, final Object nonNull
            , final Class<T> targetClass) throws UnsupportedConvertingException {
        throw createNotSupportedException(indexBaseZero, targetClass);
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    @SuppressWarnings("unchecked")
    protected <T extends Enum<T>> T convertToEnum(final int indexBaseZero, final Object nonNull
            , final Class<?> enumClass) throws UnsupportedConvertingException {
        final T value;
        try {
            final Class<T> clazz = (Class<T>) enumClass;
            if (nonNull instanceof Integer && CodeEnum.class.isAssignableFrom(enumClass)) {
                value = (T) CodeEnum.resolve(enumClass, (Integer) nonNull);
                if (value == null) {
                    throw createNotSupportedException(indexBaseZero, enumClass);
                }
            } else if (nonNull instanceof String) {
                value = Enum.valueOf(clazz, (String) nonNull);
            } else if (nonNull instanceof byte[]) {
                String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = Enum.valueOf(clazz, textValue);
            } else {
                throw createNotSupportedException(indexBaseZero, enumClass);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, enumClass);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected byte[] convertToByteArray(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        final byte[] value;

        if (nonNull instanceof byte[]) {
            value = (byte[]) nonNull;
        } else if (nonNull instanceof String) {
            value = ((String) nonNull).getBytes(obtainColumnCharset(indexBaseZero));
        } else if (nonNull instanceof LongBinary) {
            LongBinary longBinary = (LongBinary) nonNull;
            if (longBinary.isArray()) {
                value = longBinary.asArray();
            } else {
                throw createNotSupportedException(indexBaseZero, byte[].class);
            }
        } else if (nonNull instanceof LongString) {
            final LongString v = (LongString) nonNull;
            if (v.isString()) {
                value = v.asString().getBytes(obtainColumnCharset(indexBaseZero));
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
    protected String convertToString(final int indexBaseZero, final Object nonNull) {
        try {
            final String value;

            if (nonNull instanceof String) {
                value = (String) nonNull;
            } else if (nonNull instanceof byte[]) {
                value = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            } else if (nonNull instanceof Enum) {
                value = ((Enum<?>) nonNull).name();
            } else if (nonNull instanceof Long) {
                if (this.rowMeta.getSQLType(indexBaseZero).jdbcType() == JDBCType.BIT) {
                    value = new StringBuilder(Long.toBinaryString((Long) nonNull))
                            .reverse().toString();
                } else {
                    value = Long.toString((Long) nonNull);
                }
            } else if (nonNull instanceof BigDecimal) {
                value = ((BigDecimal) nonNull).toPlainString();
            } else if (nonNull instanceof LongString) {
                final LongString v = (LongString) nonNull;
                if (v.isString()) {
                    value = v.asString();
                } else {
                    throw createNotSupportedException(indexBaseZero, String.class);
                }
            } else if (nonNull instanceof LongBinary) {
                final LongBinary v = (LongBinary) nonNull;
                if (v.isArray()) {
                    value = new String(v.asArray(), obtainColumnCharset(indexBaseZero));
                } else {
                    throw createNotSupportedException(indexBaseZero, String.class);
                }
            } else if (nonNull instanceof BitSet) {
                value = JdbdStrings.bitSetToBitString((BitSet) nonNull, false);
            } else if (nonNull instanceof LocalTime) {
                value = ((LocalTime) nonNull).format(JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            } else if (nonNull instanceof LocalDate) {
                value = ((LocalDate) nonNull).format(DateTimeFormatter.ISO_LOCAL_DATE);
            } else if (nonNull instanceof LocalDateTime) {
                value = ((LocalDateTime) nonNull).format(JdbdTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } else if (nonNull instanceof OffsetTime) {
                value = ((OffsetTime) nonNull).format(JdbdTimes.ISO_OFFSET_TIME_FORMATTER);
            } else if (nonNull instanceof OffsetDateTime) {
                value = ((OffsetDateTime) nonNull).format(JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else if (nonNull instanceof ZonedDateTime) {
                value = ((ZonedDateTime) nonNull).format(JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                value = nonNull.toString();
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
    protected final Duration convertToDuration(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        final Duration value;
        try {
            if (nonNull instanceof Interval) {
                value = ((Interval) nonNull).toDurationExact();
            } else if (nonNull instanceof TemporalAmount) {
                value = Duration.from((TemporalAmount) nonNull);
            } else {
                final Interval v;
                if (nonNull instanceof String) {
                    v = Interval.parse((String) nonNull, true);
                } else if (nonNull instanceof byte[]) {
                    final String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                    v = Interval.parse(textValue, true);
                } else {
                    throw createNotSupportedException(indexBaseZero, Duration.class);
                }
                value = v.toDurationExact();
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
    protected final Period convertToPeriod(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final Period value;
            if (nonNull instanceof Interval) {
                value = ((Interval) nonNull).toPeriodExact();
            } else if (nonNull instanceof TemporalAmount) {
                value = Period.from((TemporalAmount) nonNull);
            } else {
                final Interval v;
                if (nonNull instanceof String) {
                    v = Interval.parse((String) nonNull, true);
                } else if (nonNull instanceof byte[]) {
                    final String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                    v = Interval.parse(textValue, true);
                } else {
                    throw createNotSupportedException(indexBaseZero, Period.class);
                }
                value = v.toPeriodExact();
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
    protected final Interval convertToInterval(final int indexBaseZero, final Object nonNull) {
        final Interval value;
        if (nonNull instanceof Duration) {
            value = Interval.of((Duration) nonNull);
        } else if (nonNull instanceof Period) {
            value = Interval.of((Period) nonNull);
        } else if (nonNull instanceof String) {
            try {
                value = Interval.parse((String) nonNull);
            } catch (DateTimeException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Interval.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, Interval.class);
        }
        return value;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final OffsetTime convertToOffsetTime(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        if (nonNull instanceof OffsetTime) {
            return (OffsetTime) nonNull;
        }
        final String v;
        if (nonNull instanceof String) {
            v = (String) nonNull;
        } else if (nonNull instanceof byte[]) {
            v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
        } else {
            throw createNotSupportedException(indexBaseZero, OffsetTime.class);
        }
        try {
            return OffsetTime.parse(v, JdbdTimes.ISO_OFFSET_TIME_FORMATTER);
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, OffsetTime.class);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final Instant convertToInstant(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final Instant value;
            if (nonNull instanceof OffsetDateTime) {
                value = Instant.from((OffsetDateTime) nonNull);
            } else if (nonNull instanceof ZonedDateTime) {
                value = Instant.from((ZonedDateTime) nonNull);
            } else if (nonNull instanceof Long) {
                value = Instant.ofEpochMilli((Long) nonNull);
            } else {
                throw createNotSupportedException(indexBaseZero, Instant.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Instant.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final Year convertToYear(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final Year value;
            if (nonNull instanceof Year) {
                value = (Year) nonNull;
            } else if (nonNull instanceof Integer) {
                value = Year.of((Integer) nonNull);
            } else if (nonNull instanceof Short) {
                value = Year.of((Short) nonNull);
            } else {
                final int v;
                if (nonNull instanceof String) {
                    v = Integer.parseInt((String) nonNull);
                } else if (nonNull instanceof byte[]) {
                    v = Integer.parseInt(new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero)));
                } else {
                    throw createNotSupportedException(indexBaseZero, Year.class);
                }
                value = Year.of(v);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, Year.class);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final YearMonth convertToYearMonth(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        try {
            final YearMonth value;
            if (nonNull instanceof YearMonth) {
                value = (YearMonth) nonNull;
            } else if (nonNull instanceof LocalDate) {
                value = YearMonth.from((LocalDate) nonNull);
            } else {
                value = YearMonth.from(convertToLocalDate(indexBaseZero, nonNull));
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, YearMonth.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final MonthDay convertToMonthDay(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        try {
            final MonthDay value;
            if (nonNull instanceof MonthDay) {
                value = (MonthDay) nonNull;
            } else if (nonNull instanceof LocalDate) {
                value = MonthDay.from((LocalDate) nonNull);
            } else {
                value = MonthDay.from(convertToLocalDate(indexBaseZero, nonNull));
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, MonthDay.class);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final OffsetDateTime convertToOffsetDateTime(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        try {
            final OffsetDateTime value;
            if (nonNull instanceof OffsetDateTime) {
                value = (OffsetDateTime) nonNull;
            } else if (nonNull instanceof ZonedDateTime) {
                value = ((ZonedDateTime) nonNull).toOffsetDateTime();
            } else if (nonNull instanceof String) {
                value = OffsetDateTime.parse((String) nonNull, JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = OffsetDateTime.parse(v, JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                throw createNotSupportedException(indexBaseZero, OffsetDateTime.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, OffsetDateTime.class);
        }

    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final ZonedDateTime convertToZonedDateTime(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        try {
            final ZonedDateTime value;
            if (nonNull instanceof ZonedDateTime) {
                value = (ZonedDateTime) nonNull;
            } else if (nonNull instanceof OffsetDateTime) {
                value = ((OffsetDateTime) nonNull).toZonedDateTime();
            } else if (nonNull instanceof String) {
                value = ZonedDateTime.parse((String) nonNull, JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = ZonedDateTime.parse(v, JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                throw createNotSupportedException(indexBaseZero, ZonedDateTime.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, ZonedDateTime.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final LocalTime convertToLocalTime(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final LocalTime value;
            if (nonNull instanceof LocalTime) {
                value = (LocalTime) nonNull;
            } else if (nonNull instanceof String) {
                value = LocalTime.parse((String) nonNull, JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = LocalTime.parse(v, JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            } else {
                throw createNotSupportedException(indexBaseZero, LocalTime.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalTime.class);
        }
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final LocalDateTime convertToLocalDateTime(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final LocalDateTime value;
            if (nonNull instanceof LocalDateTime) {
                value = (LocalDateTime) nonNull;
            } else if (nonNull instanceof String) {
                value = LocalDateTime.parse((String) nonNull, JdbdTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = LocalDateTime.parse(v, JdbdTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } else {
                throw createNotSupportedException(indexBaseZero, LocalDateTime.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalDateTime.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final LocalDate convertToLocalDate(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        try {
            final LocalDate value;
            if (nonNull instanceof LocalDate) {
                value = (LocalDate) nonNull;
            } else if (nonNull instanceof String) {
                value = LocalDate.parse((String) nonNull);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = LocalDate.parse(v);
            } else {
                throw createNotSupportedException(indexBaseZero, LocalDate.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, LocalDate.class);
        }

    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final double convertToDouble(final int indexBaseZero, final Object nonNull) {
        final double value;
        if (nonNull instanceof Double
                || nonNull instanceof Float) {
            value = ((Number) nonNull).doubleValue();
        } else if (nonNull instanceof String) {
            try {
                value = Double.parseDouble((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Double.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Double.parseDouble(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Double.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0D : 0.0D;
        } else {
            throw createNotSupportedException(indexBaseZero, Double.class);
        }
        return value;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final float convertToFloat(final int indexBaseZero, final Object nonNull) {
        final float value;
        if (nonNull instanceof Float) {
            value = (Float) nonNull;
        } else if (nonNull instanceof String) {
            try {
                value = Float.parseFloat((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Float.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Float.parseFloat(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Float.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0F : 0.0F;
        } else {
            throw createNotSupportedException(indexBaseZero, Float.class);
        }
        return value;
    }


    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final BigDecimal convertToBigDecimal(final int indexBaseZero, final Object nonNull) {

        try {
            final BigDecimal value;
            if (nonNull instanceof Number) {
                if (nonNull instanceof BigInteger) {
                    value = new BigDecimal((BigInteger) nonNull);
                } else if (nonNull instanceof Integer
                        || nonNull instanceof Long
                        || nonNull instanceof Short
                        || nonNull instanceof Byte) {
                    value = BigDecimal.valueOf(((Number) nonNull).longValue());
                } else {
                    throw createNotSupportedException(indexBaseZero, BigDecimal.class);
                }
            } else if (nonNull instanceof String) {
                value = new BigDecimal((String) nonNull);
            } else if (nonNull instanceof byte[]) {
                final String v = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
                value = new BigDecimal(v);
            } else if (nonNull instanceof Boolean) {
                value = ((Boolean) nonNull) ? BigDecimal.ONE : BigDecimal.ZERO;
            } else {
                throw createNotSupportedException(indexBaseZero, BigDecimal.class);
            }
            return value;
        } catch (UnsupportedConvertingException e) {
            throw e;
        } catch (Throwable e) {
            throw createValueCannotConvertException(e, indexBaseZero, BigDecimal.class);
        }
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final BigInteger convertToBigInteger(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {

        final BigInteger value;
        if (nonNull instanceof BigInteger) {
            value = (BigInteger) nonNull;
        } else if (nonNull instanceof Integer
                || nonNull instanceof Long
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = BigInteger.valueOf(((Number) nonNull).longValue());
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() != 0) {
                throw createNotSupportedException(indexBaseZero, BigInteger.class);
            }
            value = v.toBigInteger();
        } else if (nonNull instanceof String) {
            try {
                value = new BigInteger((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigInteger.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = new BigInteger(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, BigInteger.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? BigInteger.ONE : BigInteger.ZERO;
        } else {
            throw createNotSupportedException(indexBaseZero, BigInteger.class);
        }
        return value;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final long convertToLong(final int indexBaseZero, final Object nonNull) {
        final long value;
        if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).longValue();
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() != 0
                    || v.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0
                    || v.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Long.class, Long.MIN_VALUE, Long.MAX_VALUE);
            }
            value = v.longValue();
        } else if (nonNull instanceof BigInteger) {
            final BigInteger v = (BigInteger) nonNull;
            if (v.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0
                    || v.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Long.class, Long.MIN_VALUE, Long.MAX_VALUE);
            }
            value = v.longValue();
        } else if (nonNull instanceof String) {
            try {
                value = Long.parseLong((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Long.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Long.parseLong(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Long.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1L : 0L;
        } else if (nonNull instanceof BitSet) {
            final BitSet v = (BitSet) nonNull;
            if (v.length() < 65) {
                value = v.toLongArray()[0];
            } else {
                throw createNotSupportedException(indexBaseZero, Long.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, Long.class);
        }
        return value;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final int convertToInteger(final int indexBaseZero, final Object nonNull) {
        final int value;

        if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).intValue();
        } else if (nonNull instanceof String) {
            try {
                value = Integer.parseInt((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Integer.class);
            }
        } else if (nonNull instanceof Long) {
            final long v = (Long) nonNull;
            if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE);
            }
            value = (int) v;
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() != 0
                    || v.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0
                    || v.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE);
            }
            value = v.intValue();
        } else if (nonNull instanceof BigInteger) {
            final BigInteger v = (BigInteger) nonNull;
            if (v.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0
                    || v.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE);
            }
            value = v.intValue();
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Integer.parseInt(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Integer.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1 : 0;
        } else if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof BitSet) {
            final BitSet v = (BitSet) nonNull;
            if (v.length() < 33) {
                value = (int) v.toLongArray()[0];
            } else {
                throw createNotSupportedException(indexBaseZero, Integer.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, Integer.class);
        }

        return value;

    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final short convertToShort(final int indexBaseZero, final Object nonNull) {
        final short value;

        if (nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).shortValue();
        } else if (nonNull instanceof Integer) {
            final int v = (Integer) nonNull;
            if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            value = (short) v;
        } else if (nonNull instanceof Long) {
            final long v = (Long) nonNull;
            if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            value = (short) v;
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() != 0
                    || v.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0
                    || v.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            value = v.shortValue();
        } else if (nonNull instanceof BigInteger) {
            final BigInteger v = (BigInteger) nonNull;
            if (v.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0
                    || v.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            value = v.shortValue();
        } else if (nonNull instanceof String) {
            try {
                value = Short.parseShort((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Short.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Short.parseShort(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Short.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? (short) 1 : (short) 0;
        } else if (nonNull instanceof Year) {
            final int v = ((Year) nonNull).getValue();
            if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            value = (short) v;
        } else {
            throw createNotSupportedException(indexBaseZero, Short.class);
        }
        return value;
    }

    /**
     * @see #convertNonNullValue(int, Object, Class)
     */
    protected final byte convertToByte(final int indexBaseZero, final Object nonNull) {
        final byte value;
        if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            final int v = ((Number) nonNull).intValue();
            if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE);
            }
            value = (byte) v;
        } else if (nonNull instanceof Long) {
            final long v = (Long) nonNull;
            if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
                throw createOutOfRangeException(indexBaseZero, Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE);
            }
            value = (byte) v;
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() != 0
                    || v.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0
                    || v.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE);
            }
            value = v.byteValue();
        } else if (nonNull instanceof BigInteger) {
            final BigInteger v = (BigInteger) nonNull;
            if (v.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0
                    || v.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0) {
                throw createOutOfRangeException(indexBaseZero, Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE);
            }
            value = v.byteValue();
        } else if (nonNull instanceof String) {
            try {
                value = Byte.parseByte((String) nonNull);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Byte.class);
            }
        } else if (nonNull instanceof byte[]) {
            String textValue = new String((byte[]) nonNull, obtainColumnCharset(indexBaseZero));
            try {
                value = Byte.parseByte(textValue);
            } catch (NumberFormatException e) {
                throw createValueCannotConvertException(e, indexBaseZero, Byte.class);
            }
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? (byte) 1 : (byte) 0;
        } else {
            throw createNotSupportedException(indexBaseZero, Byte.class);
        }
        return value;
    }


    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause
            , int indexBasedZero, Class<?> targetClass) {
        SQLType sqlType = this.rowMeta.getSQLType(indexBasedZero);

        String message = String.format("Not support convert from column(index[%s] label[%s] and sql type[%s]) to %s.",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , sqlType, targetClass.getName());

        return new UnsupportedConvertingException(message, cause, sqlType, targetClass);
    }


    protected UnsupportedConvertingException createNotSupportedException(int indexBasedZero
            , Class<?> targetClass) {
        SQLType sqlType = this.rowMeta.getSQLType(indexBasedZero);

        String message = String.format("Not support convert from column(index[%s] label[%s] and sql type[%s]) to %s.",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , sqlType, targetClass.getName());

        return new UnsupportedConvertingException(message, sqlType, targetClass);
    }

    protected UnsupportedConvertingException createOutOfRangeException(int indexBasedZero
            , Class<?> targetClass, Number min, Number max) {
        SQLType sqlType = this.rowMeta.getSQLType(indexBasedZero);

        String message;
        message = String.format("Not support convert from (index[%s] label[%s] and sql type[%s]) to %s,out of [%s,%s]",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , sqlType, targetClass.getName(), min, max);
        return new UnsupportedConvertingException(message, sqlType, targetClass);
    }


    /*################################## blow private method ##################################*/

    private int checkIndex(final int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnValues.length) {
            final String label = this.rowMeta.getColumnLabel(indexBaseZero);
            throw new JdbdSQLException(new SQLException(
                    String.format("Column[index:%s,label:%s] out of bounds[0 , %s)."
                            , indexBaseZero, label, columnValues.length)));
        }
        return indexBaseZero;
    }


}
