package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRow;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import org.qinarmy.util.NotSupportedConvertException;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

abstract class MySQLResultRow implements ResultRow {

    static MySQLResultRow from(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        return new SimpleMySQLResultRow(columnValues, rowMeta, adjutant);
    }

    private static final Map<Class<?>, Function<Object, Object>> COLUMN_CONVERTER_MAP = createColumnConverterMap();

    private final Object[] columnValues;

    private final MySQLRowMeta rowMeta;

    private final ResultRowAdjutant adjutant;

    private MySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        if (columnValues.length != rowMeta.columnMetas.length) {
            throw new IllegalArgumentException(
                    String.format("columnValues length[%s] and columnMetas of rowMeta length[%s] not match."
                            , columnValues.length, rowMeta.columnMetas.length));
        }
        this.columnValues = columnValues;
        this.rowMeta = rowMeta;
        this.adjutant = adjutant;
    }

    @Nullable
    @Override
    public final Object getObject(int indexBaseZero) throws ReactiveSQLException {
        return this.columnValues[checkIndex(indexBaseZero)];
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public final <T> T getObject(int indexBaseZero, Class<T> columnClass) throws ReactiveSQLException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        return (value == null || columnClass.isInstance(value))
                ? (T) value
                : convertValue(value, columnClass);
    }

    @Nullable
    @Override
    public final Object getObject(String alias) throws ReactiveSQLException {
        try {
            return this.columnValues[checkIndex(rowMeta.convertToIndex(alias))];
        } catch (ReactiveSQLException e) {
            throw new ReactiveSQLException(String.format("alias[%s] access error.", alias), e);
        }
    }

    @Nullable
    @Override
    public final <T> T getObject(String alias, Class<T> columnClass) throws ReactiveSQLException {
        try {
            return getObject(this.rowMeta.convertToIndex(alias), columnClass);
        } catch (ReactiveSQLException e) {
            throw new ReactiveSQLException(String.format("alias[%s] access error.", alias), e);
        }
    }

    @Override
    public final <T extends Temporal> T getObject(int indexBaseZero, Class<T> targetClass, ZoneId targetZoneId)
            throws ReactiveSQLException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        T newValue;
        if (value instanceof LocalDateTime) {
            newValue = convertFromLocalDateTime((LocalDateTime) value, targetClass, targetZoneId);
        } else if (value instanceof LocalTime) {
            newValue = convertFromLocalTime((LocalTime) value, targetClass, targetZoneId);
        } else if (value instanceof ZonedDateTime) {
            newValue = convertFromZonedDateTime((ZonedDateTime) value, targetClass, targetZoneId);
        } else if (value instanceof OffsetDateTime) {
            newValue = convertFromOffsetDateTime((OffsetDateTime) value, targetClass, targetZoneId);
        } else if (value instanceof OffsetTime) {
            newValue = convertFromOffsetTime((OffsetTime) value, targetClass, targetZoneId);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return newValue;
    }

    @Nullable
    @Override
    public final <T extends Temporal> T getObject(String alias, Class<T> targetClass, ZoneId zoneId)
            throws ReactiveSQLException {
        try {
            return getObject(this.rowMeta.convertToIndex(alias), targetClass, zoneId);
        } catch (ReactiveSQLException e) {
            throw new ReactiveSQLException(String.format("alias[%s] access error.", alias), e);
        }
    }

    private int checkIndex(int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnValues.length) {
            throw new ReactiveSQLException(new SQLException(
                    String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnValues.length - 1)));
        }
        return indexBaseZero;
    }

    @SuppressWarnings("unchecked")
    private <T> T convertValue(Object nonValue, Class<T> columnClass) {
        Function<Object, Object> function = COLUMN_CONVERTER_MAP.get(columnClass);
        if (function == null) {
            throw createNotSupportedException(nonValue, columnClass);
        }
        return (T) function.apply(nonValue);
    }

    /*################################## blow instance converter method ##################################*/

    @SuppressWarnings("unchecked")
    private <T extends Temporal> T convertFromLocalDateTime(LocalDateTime value, Class<T> targetClass
            , ZoneId targetZoneId) {
        // @see AbstractClientProtocol#toLocalDateTime(ByteBuf, MySQLColumnMeta)
        OffsetDateTime newOffset = OffsetDateTime.of(value, this.adjutant.obtainClientZoneOffset())
                .withOffsetSameInstant(MySQLTimeUtils.toZoneOffset(targetZoneId));

        Temporal newValue;
        if (targetClass == ZonedDateTime.class) {
            newValue = newOffset.toZonedDateTime();
        } else if (targetClass == OffsetDateTime.class) {
            newValue = newOffset;
        } else if (targetClass == LocalDateTime.class) {
            newValue = newOffset.toLocalDateTime();
        } else if (targetClass == LocalDate.class) {
            newValue = newOffset.toLocalDate();
        } else if (targetClass == OffsetTime.class) {
            newValue = newOffset.toOffsetTime();
        } else if (targetClass == LocalTime.class) {
            newValue = newOffset.toLocalTime();
        } else if (targetClass == Year.class) {
            newValue = Year.from(newOffset);
        } else if (targetClass == YearMonth.class) {
            newValue = YearMonth.from(newOffset);
        } else if (targetClass == Instant.class) {
            newValue = Instant.from(newOffset);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return (T) newValue;
    }

    @SuppressWarnings("unchecked")
    private <T extends Temporal> T convertFromLocalTime(LocalTime value, Class<T> targetClass
            , ZoneId targetZoneId) {
        //  @see AbstractClientProtocol#toLocalTime(ByteBuf, MySQLColumnMeta)
        OffsetTime newOffset = OffsetTime.of(value, this.adjutant.obtainClientZoneOffset())
                .withOffsetSameInstant(MySQLTimeUtils.toZoneOffset(targetZoneId));
        Temporal newValue;
        if (targetClass == OffsetTime.class) {
            newValue = newOffset;
        } else if (targetClass == LocalTime.class) {
            newValue = newOffset.toLocalTime();
        } else if (targetClass == Instant.class) {
            newValue = Instant.from(newOffset);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return (T) newValue;
    }

    @SuppressWarnings("unchecked")
    private <T extends Temporal> T convertFromZonedDateTime(ZonedDateTime value, Class<T> targetClass
            , ZoneId targetZoneId) {
        ZonedDateTime newDateTime = value.withZoneSameInstant(MySQLTimeUtils.toZoneOffset(targetZoneId));

        Temporal newValue;
        if (targetClass == ZonedDateTime.class) {
            newValue = newDateTime;
        } else if (targetClass == OffsetDateTime.class) {
            newValue = newDateTime.toOffsetDateTime();
        } else if (targetClass == LocalDateTime.class) {
            newValue = newDateTime.toLocalDateTime();
        } else if (targetClass == LocalDate.class) {
            newValue = newDateTime.toLocalDate();
        } else if (targetClass == OffsetTime.class) {
            newValue = newDateTime.toOffsetDateTime().toOffsetTime();
        } else if (targetClass == LocalTime.class) {
            newValue = newDateTime.toLocalTime();
        } else if (targetClass == Year.class) {
            newValue = Year.from(newDateTime);
        } else if (targetClass == YearMonth.class) {
            newValue = YearMonth.from(newDateTime);
        } else if (targetClass == Instant.class) {
            newValue = Instant.from(newDateTime);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return (T) newValue;
    }

    @SuppressWarnings("unchecked")
    private <T extends Temporal> T convertFromOffsetDateTime(OffsetDateTime value, Class<T> targetClass
            , ZoneId targetZoneId) {
        OffsetDateTime newOffset = value.withOffsetSameInstant(MySQLTimeUtils.toZoneOffset(targetZoneId));

        Temporal newValue;
        if (targetClass == ZonedDateTime.class) {
            newValue = newOffset.toZonedDateTime();
        } else if (targetClass == OffsetDateTime.class) {
            newValue = newOffset;
        } else if (targetClass == LocalDateTime.class) {
            newValue = newOffset.toLocalDateTime();
        } else if (targetClass == LocalDate.class) {
            newValue = newOffset.toLocalDate();
        } else if (targetClass == OffsetTime.class) {
            newValue = newOffset.toOffsetTime();
        } else if (targetClass == LocalTime.class) {
            newValue = newOffset.toLocalTime();
        } else if (targetClass == Year.class) {
            newValue = Year.from(newOffset);
        } else if (targetClass == YearMonth.class) {
            newValue = YearMonth.from(newOffset);
        } else if (targetClass == Instant.class) {
            newValue = Instant.from(newOffset);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return (T) newValue;
    }

    @SuppressWarnings("unchecked")
    private <T extends Temporal> T convertFromOffsetTime(OffsetTime value, Class<T> targetClass
            , ZoneId targetZoneId) {
        OffsetTime newOffset = value.withOffsetSameInstant(MySQLTimeUtils.toZoneOffset(targetZoneId));
        Temporal newValue;
        if (targetClass == OffsetTime.class) {
            newValue = newOffset;
        } else if (targetClass == LocalTime.class) {
            newValue = newOffset.toLocalTime();
        } else if (targetClass == Instant.class) {
            newValue = Instant.from(newOffset);
        } else {
            throw createNotSupportedException(value, targetClass);
        }
        return (T) newValue;
    }


    /*################################## blow static converter method ##################################*/

    private static OffsetTime convertToOffsetTime(Object sourceValue) {
        OffsetTime newValue;
        if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toOffsetDateTime().toOffsetTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toOffsetTime();
        } else {
            throw createNotSupportedException(sourceValue, OffsetTime.class);
        }
        return newValue;
    }

    private static OffsetDateTime convertToOffsetDateTime(Object sourceValue) {
        if (sourceValue instanceof ZonedDateTime) {
            return ((ZonedDateTime) sourceValue).toOffsetDateTime();
        }
        throw createNotSupportedException(sourceValue, OffsetDateTime.class);
    }


    private static ZonedDateTime convertToZonedDateTime(Object sourceValue) {
        if (sourceValue instanceof OffsetDateTime) {
            return ((OffsetDateTime) sourceValue).toZonedDateTime();
        }
        throw createNotSupportedException(sourceValue, ZonedDateTime.class);
    }


    private static LocalTime convertToLocalTime(Object sourceValue) {
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
            throw createNotSupportedException(sourceValue, LocalTime.class);
        }
        return newValue;
    }

    private static LocalDateTime convertToLocalDateTime(Object sourceValue) {
        LocalDateTime newValue;
        if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toLocalDateTime();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toLocalDateTime();
        } else {
            throw createNotSupportedException(sourceValue, LocalDateTime.class);
        }
        return newValue;
    }


    private static LocalDate convertToLocalDate(Object sourceValue) {
        LocalDate newValue;
        if (sourceValue instanceof LocalDateTime) {
            newValue = ((LocalDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof ZonedDateTime) {
            newValue = ((ZonedDateTime) sourceValue).toLocalDate();
        } else if (sourceValue instanceof OffsetDateTime) {
            newValue = ((OffsetDateTime) sourceValue).toLocalDate();
        } else {
            throw createNotSupportedException(sourceValue, LocalDate.class);
        }
        return newValue;
    }


    private static Boolean convertToBoolean(Object sourceValue) {
        boolean newValue;
        if (sourceValue instanceof String) {
            Boolean boolValue = MySQLStringUtils.tryConvertToBoolean((String) sourceValue);
            if (boolValue == null) {
                throw createNotSupportedException(sourceValue, Boolean.class);
            }
            newValue = boolValue;
        } else if (sourceValue instanceof Number) {
            if (sourceValue instanceof Long) {
                // most probably for mysql
                // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
                // @see io.jdbd.mysql.protocol.client.AbstractClientProtocol.toBoolean
                newValue = (Long) sourceValue != 0L;
            } else if (sourceValue instanceof Float || sourceValue instanceof Double) {
                //this means that 0.1 or -1 will be TRUE
                // @see io.jdbd.mysql.protocol.client.AbstractClientProtocol.toBoolean
                newValue = ((Number) sourceValue).doubleValue() != 0.0d;
            } else if (sourceValue instanceof BigDecimal) {
                //this means that 0.1 or -1 will be TRUE
                newValue = ((BigDecimal) sourceValue).compareTo(BigDecimal.ZERO) != 0;
            } else if (sourceValue instanceof Byte || sourceValue instanceof Short) {
                newValue = ((Number) sourceValue).intValue() != 0;
            } else if (sourceValue instanceof BigInteger) {
                newValue = ((BigInteger) sourceValue).compareTo(BigInteger.valueOf(0L)) != 0;
            } else {
                throw createNotSupportedException(sourceValue, Boolean.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, Boolean.class);
        }

        return newValue;
    }


    private static Double convertToDouble(Object sourceValue) {
        double newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Double.parseDouble((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, Double.class);
            }
        } else if (sourceValue instanceof Float) {
            newValue = ((Float) sourceValue).doubleValue();
        } else {
            throw createNotSupportedException(sourceValue, BigDecimal.class);
        }
        return newValue;
    }

    private static BigDecimal convertToBigDecimal(Object sourceValue) {
        BigDecimal newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigDecimal((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, BigDecimal.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToBigDecimal((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, BigDecimal.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, BigDecimal.class);
        }
        return newValue;
    }

    private static BigInteger convertToBigInteger(Object sourceValue) {
        BigInteger newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = new BigInteger((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, BigInteger.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToBigInteger((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, BigInteger.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, BigInteger.class);
        }
        return newValue;
    }

    private static Long convertToLong(Object sourceValue) {
        long newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Long.parseLong((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, Long.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToLong((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, Long.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, Long.class);
        }
        return newValue;
    }

    private static Integer convertToInteger(Object sourceValue) {
        int newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Integer.parseInt((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, Integer.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToInt((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, Integer.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, Integer.class);
        }
        return newValue;
    }

    private static Short convertToShort(Object sourceValue) {
        short newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Short.parseShort((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, Short.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToShort((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, Short.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, Short.class);
        }
        return newValue;
    }

    private static Byte convertTotByte(Object sourceValue) {
        byte newValue;
        if (sourceValue instanceof String) {
            try {
                newValue = Byte.parseByte((String) sourceValue);
            } catch (NumberFormatException e) {
                throw createNotSupportedException(sourceValue, Byte.class);
            }
        } else if (sourceValue instanceof Number) {
            try {
                newValue = MySQLNumberUtils.convertNumberToByte((Number) sourceValue);
            } catch (NotSupportedConvertException e) {
                throw createNotSupportedException(sourceValue, Byte.class);
            }
        } else {
            throw createNotSupportedException(sourceValue, Byte.class);
        }
        return newValue;
    }

    /**
     * @return a unmodifiable map
     */
    private static Map<Class<?>, Function<Object, Object>> createColumnConverterMap() {
        Map<Class<?>, Function<Object, Object>> map;

        map = new HashMap<>();

        map.put(Boolean.class, MySQLResultRow::convertToBoolean);
        map.put(Double.class, MySQLResultRow::convertToDouble);         // 1 four
        map.put(BigDecimal.class, MySQLResultRow::convertToBigDecimal);
        map.put(BigInteger.class, MySQLResultRow::convertToBigInteger);

        map.put(Long.class, MySQLResultRow::convertToLong);
        map.put(Integer.class, MySQLResultRow::convertToInteger);       // 2 four
        map.put(Short.class, MySQLResultRow::convertToShort);
        map.put(Byte.class, MySQLResultRow::convertTotByte);

        map.put(LocalDate.class, MySQLResultRow::convertToLocalDate);
        map.put(LocalDateTime.class, MySQLResultRow::convertToLocalDateTime);      // 3 four
        map.put(LocalTime.class, MySQLResultRow::convertToLocalTime);
        map.put(ZonedDateTime.class, MySQLResultRow::convertToZonedDateTime);

        map.put(OffsetDateTime.class, MySQLResultRow::convertToOffsetDateTime);
        map.put(OffsetTime.class, MySQLResultRow::convertToOffsetTime);

        return Collections.unmodifiableMap(map);
    }


    private static ReactiveSQLException createNotSupportedException(Object value, Class<?> targetClass) {
        String m = String.format("Not support convert from value[%s] and type[%s] to [%s] ."
                , value
                , value.getClass().getName(), targetClass.getName());
        return new ReactiveSQLException(new SQLException(m));
    }

    private static final class SimpleMySQLResultRow extends MySQLResultRow {

        private SimpleMySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
            super(columnValues, rowMeta, adjutant);
        }
    }
}
