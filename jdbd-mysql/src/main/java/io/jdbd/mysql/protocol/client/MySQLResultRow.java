package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import org.qinarmy.util.NotSupportedConvertException;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;

abstract class MySQLResultRow implements ResultRow {

    static MySQLResultRow from(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        return new SimpleMySQLResultRow(columnValues, rowMeta, adjutant);
    }


    private final Object[] columnValues;

    private final MySQLRowMeta rowMeta;

    private final ResultRowAdjutant adjutant;

    private MySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        if (columnValues.length != rowMeta.columnMetaArray.length) {
            throw new IllegalArgumentException(
                    String.format("columnValues length[%s] and columnMetas of rowMeta length[%s] not match."
                            , columnValues.length, rowMeta.columnMetaArray.length));
        }
        this.columnValues = columnValues;
        this.rowMeta = rowMeta;
        this.adjutant = adjutant;
    }

    @Override
    public ResultRowMeta obtainRowMeta() {
        return this.rowMeta;
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
    public final Object get(String alias) throws JdbdSQLException {
        try {
            return this.columnValues[checkIndex(rowMeta.convertToIndex(alias))];
        } catch (MySQLJdbdException e) {
            throw new MySQLJdbdException(String.format("alias[%s] access error.", alias), e);
        }
    }

    @Nullable
    @Override
    public final <T> T get(String alias, Class<T> columnClass) throws JdbdSQLException {
        try {
            return get(this.rowMeta.convertToIndex(alias), columnClass);
        } catch (MySQLJdbdException e) {
            throw new MySQLJdbdException(String.format("alias[%s] access error.", alias), e);
        }
    }


    @Override
    public Object obtain(int indexBaseZero) throws JdbdSQLException {
        Object value = get(indexBaseZero);
        if (value == null) {
            throw createNotRequiredException(indexBaseZero);
        }
        return value;
    }

    @Override
    public <T> T obtain(int indexBaseZero, Class<T> columnClass) throws JdbdSQLException {
        T value = get(indexBaseZero, columnClass);
        if (value == null) {
            throw createNotRequiredException(indexBaseZero);
        }
        return value;
    }

    @Override
    public Object obtain(String alias) throws JdbdSQLException {
        Object value = get(alias);
        if (value == null) {
            throw createNotRequiredException(alias);
        }
        return value;
    }

    @Override
    public <T> T obtain(String alias, Class<T> columnClass) throws JdbdSQLException {
        T value = get(alias, columnClass);
        if (value == null) {
            throw createNotRequiredException(alias);
        }
        return value;
    }



    /*################################## blow private method ##################################*/

    private int checkIndex(int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnValues.length) {
            throw new JdbdSQLException(new SQLException(
                    String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnValues.length - 1)));
        }
        return indexBaseZero;
    }

    @SuppressWarnings("unchecked")
    private <T> T convertValue(final int indexBaseZero, final Object nonValue, final Class<T> targetClass) {
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
        } else if (Temporal.class.isAssignableFrom(targetClass)) {
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
            } else {
                throw createNotSupportedException(indexBaseZero, nonValue.getClass(), targetClass);
            }
        } else if (targetClass == MonthDay.class) {
            convertedValue = convertToMonthDay(indexBaseZero, nonValue);
        } else if (targetClass == DayOfWeek.class) {
            convertedValue = convertToDayOfWeek(indexBaseZero, nonValue);
        } else {
            throw createNotSupportedException(indexBaseZero, nonValue.getClass(), targetClass);
        }
        return (T) convertedValue;
    }

    /*################################## blow instance converter method ##################################*/


    /*################################## blow static converter method ##################################*/

    /**
     * @see #convertValue(int, Object, Class)
     */
    private OffsetTime convertToOffsetTime(final int indexBaseZero, Object sourceValue) {
        OffsetTime newValue;
        if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toOffsetDateTime()
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                    .toOffsetTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue)
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                    .toOffsetTime();
        } else if (sourceValue instanceof LocalTime) {
            newValue = OffsetTime.of((LocalTime) sourceValue, this.adjutant.obtainZoneOffsetClient());
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), OffsetTime.class);
        }
        return newValue;
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    private Instant convertToInstant(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Instant.class);
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    private Year convertToYear(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Year.class);
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    private YearMonth convertToYearMonth(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), YearMonth.class);
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    private MonthDay convertToMonthDay(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), MonthDay.class);
    }

    /**
     * @see #convertValue(int, Object, Class)
     */
    private DayOfWeek convertToDayOfWeek(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), DayOfWeek.class);
    }

    private OffsetDateTime convertToOffsetDateTime(final int indexBaseZero, Object sourceValue) {
        if (sourceValue instanceof ZonedDateTime) {
            return ((ZonedDateTime) sourceValue).toOffsetDateTime();
        }
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), OffsetDateTime.class);
    }


    private ZonedDateTime convertToZonedDateTime(final int indexBaseZero, Object sourceValue) {
        if (sourceValue instanceof OffsetDateTime) {
            return ((OffsetDateTime) sourceValue).toZonedDateTime();
        }
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), ZonedDateTime.class);
    }


    private LocalTime convertToLocalTime(final int indexBaseZero, Object sourceValue) {
        LocalTime newValue;
        if (sourceValue instanceof OffsetTime) {
            newValue = ((OffsetTime) sourceValue).toLocalTime();
        } else if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalTime();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toLocalTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toLocalTime();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalTime.class);
        }
        return newValue;
    }

    private LocalDateTime convertToLocalDateTime(final int indexBaseZero, final Object sourceValue) {
        final LocalDateTime newValue;
        if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toLocalDateTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toLocalDateTime();
        } else if (sourceValue instanceof String) {
            try {
                newValue = LocalDateTime.parse((String) sourceValue, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw createValueCannotConvertException(indexBaseZero, sourceValue.getClass(), LocalDateTime.class);
            }
        } else if (sourceValue instanceof byte[]) {
            String textValue = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
            try {
                newValue = LocalDateTime.parse(textValue, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw createValueCannotConvertException(indexBaseZero, sourceValue.getClass(), LocalDateTime.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalDateTime.class);
        }
        return newValue;
    }


    private LocalDate convertToLocalDate(final int indexBaseZero, Object sourceValue) {
        LocalDate newValue;
        if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toLocalDate();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), LocalDate.class);
        }
        return newValue;
    }


    private Double convertToDouble(final int indexBaseZero, Object sourceValue) {
        double newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Double.parseDouble((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Double.class);
            }
        } else if (sourceValue instanceof Float) {
            newValue = ((Float) sourceValue).doubleValue();
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
        }
        return newValue;
    }

    private Float convertToFloat(final int indexBaseZero, Object sourceValue) {
        throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
    }

    private BigDecimal convertToBigDecimal(final int indexBaseZero, Object sourceValue) {
        BigDecimal newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigDecimal((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToBigDecimal((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigDecimal.class);
        }
        return newValue;
    }

    private BigInteger convertToBigInteger(final int indexBaseZero, Object sourceValue) {
        BigInteger newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigInteger((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigInteger.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToBigInteger((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigInteger.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), BigInteger.class);
        }
        return newValue;
    }

    private Long convertToLong(final int indexBaseZero, Object sourceValue) {
        long newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Long.parseLong((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Long.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToLong((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Long.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Long.class);
        }
        return newValue;
    }

    private Integer convertToInteger(final int indexBaseZero, Object sourceValue) {
        int newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Integer.parseInt((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Integer.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToInt((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Integer.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Integer.class);
        }
        return newValue;
    }

    private Short convertToShort(final int indexBaseZero, Object sourceValue) {
        short newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Short.parseShort((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Short.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToShort((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Short.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Short.class);
        }
        return newValue;
    }

    private Byte convertToByte(final int indexBaseZero, Object sourceValue) {
        byte newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Byte.parseByte((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Byte.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToByte((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Byte.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), Byte.class);
        }
        return newValue;
    }

    private String convertToString(final int indexBaseZero, final Object sourceValue) {
        final String value;
        if (sourceValue instanceof byte[]) {
            value = new String((byte[]) sourceValue, obtainColumnCharset(indexBaseZero));
        } else if (sourceValue instanceof BigDecimal) {
            value = ((BigDecimal) sourceValue).toPlainString();
        } else if (sourceValue instanceof Number || sourceValue instanceof Year) {
            value = sourceValue.toString();
        } else if (sourceValue instanceof LocalDateTime) {
            value = ((LocalDateTime) sourceValue).format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (sourceValue instanceof LocalTime) {
            value = ((LocalTime) sourceValue).format(MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        } else {
            throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), String.class);
        }
        return value;
    }

    private Charset obtainColumnCharset(final int index) {
        Charset charset = this.adjutant.getCharsetResults();
        if (charset == null) {
            charset = this.rowMeta.columnMetaArray[index].columnCharset;
        }
        return charset;
    }


    private static JdbdSQLException createNotRequiredException(int indexBaseZero) {
        return new JdbdSQLException(new SQLException(
                String.format("Expected Object at index[%s] non-null,but null.", indexBaseZero)));
    }

    private static JdbdSQLException createNotRequiredException(String alias) {
        return new JdbdSQLException(new SQLException(
                String.format("Expected Object at alias[%s] non-null,but null.", alias)));
    }

    private JdbdSQLException createNotSupportedException(int indexBasedZero, Class<?> valueClass
            , Class<?> targetClass) {
        String m = String.format("Not support convert from (index[%s] alias[%s] and type[%s]) to %s."
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , valueClass.getName(), targetClass.getName());
        return new JdbdSQLException(new SQLException(m));
    }

    private JdbdSQLException createValueCannotConvertException(int indexBasedZero, Class<?> valueClass
            , Class<?> targetClass) {
        String m = String.format("Cannot convert value from (index[%s] alias[%s] and type[%s]) to %s, please check value rang."
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , valueClass.getName(), targetClass.getName());
        return new JdbdSQLException(new SQLException(m));
    }

    private static final class SimpleMySQLResultRow extends MySQLResultRow {

        private SimpleMySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
            super(columnValues, rowMeta, adjutant);
        }
    }
}
