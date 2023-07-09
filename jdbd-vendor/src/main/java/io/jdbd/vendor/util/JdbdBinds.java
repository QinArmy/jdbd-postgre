package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.type.Interval;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Value;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public abstract class JdbdBinds {

    protected JdbdBinds() {
        throw new UnsupportedOperationException();
    }


    public static final int MEDIUM_INT_MIN_VALUE = 0x8000_00;
    public static final int MEDIUM_INT_MAX_VALUE = 0x7FFF_FF;


    @Deprecated
    public static boolean hasPublisher(List<? extends ParamValue> parameterGroup) {
        boolean has = false;
        for (ParamValue bindValue : parameterGroup) {
            if (bindValue.get() instanceof Publisher) {
                has = true;
                break;
            }
        }
        return has;
    }

    @Deprecated
    public static boolean hasPublisher(ParamBatchStmt stmt) {
        boolean has = false;
        for (List<? extends ParamValue> group : stmt.getGroupList()) {
            if (hasPublisher(group)) {
                has = true;
                break;
            }
        }
        return has;
    }


    @Nullable
    public static JdbdException sortAndCheckParamGroup(final int groupIndex,
                                                       final List<? extends ParamValue> paramGroup) {

        paramGroup.sort(Comparator.comparingInt(ParamValue::getIndex));

        JdbdException error = null;
        final int size = paramGroup.size();
        for (int i = 0, index; i < size; i++) {
            index = paramGroup.get(i).getIndex();
            if (index == i) {
                continue;
            }

            if (index < i) {
                error = JdbdExceptions.duplicationParameter(groupIndex, index);
            } else {
                error = JdbdExceptions.noParameterValue(groupIndex, i);
            }
            break;
        }
        return error;
    }


    public static boolean bindToBoolean(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final boolean value;
        if (nonNull instanceof Boolean) {
            value = (Boolean) nonNull;
        } else if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).intValue() != 0;
        } else if (nonNull instanceof Long) {
            value = ((Number) nonNull).longValue() != 0;
        } else if (nonNull instanceof String) {
            final String v = (String) nonNull;
            if (v.equalsIgnoreCase("TRUE")
                    || v.equalsIgnoreCase("T")) {
                value = true;
            } else if (v.equalsIgnoreCase("FALSE")
                    || v.equalsIgnoreCase("F")) {
                value = false;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            value = BigDecimal.ZERO.compareTo((BigDecimal) nonNull) != 0;
        } else if (nonNull instanceof BigInteger) {
            value = BigInteger.ZERO.compareTo((BigInteger) nonNull) != 0;
        } else if (nonNull instanceof Double
                || nonNull instanceof Float) {
            value = ((Number) nonNull).doubleValue() != 0.0;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static int bindToIntUnsigned(final int batchIndex, final Value paramValue, final int maxvalue)
            throws JdbdException, IllegalArgumentException {
        final Object nonNull = paramValue.getNonNullValue();
        final int value;
        if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else if (nonNull instanceof Long) {
            final long v = (Long) nonNull;
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = (int) v;
        } else if (nonNull instanceof String) {
            if (JdbdNumbers.isHexNumber((String) nonNull)) {
                value = Integer.decode((String) nonNull);
            } else {
                value = Integer.parseUnsignedInt((String) nonNull);
            }
        } else if (nonNull instanceof Short) {
            value = ((Short) nonNull) & 0xFFFF;
        } else if (nonNull instanceof Byte) {
            value = ((Byte) nonNull) & 0xFFFF;
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            final long v = ((BigInteger) nonNull).longValueExact();
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = (int) v;
        } else if (nonNull instanceof BigDecimal) {
            final long v = ((BigDecimal) nonNull).longValueExact();
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = (int) v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (maxvalue != -1 && (value & (~maxvalue)) != 0) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }

    public static int bindToInt(final int batchIndex, final Value paramValue, final int minValue, int maxValue)
            throws JdbdException {
        final Object nonNull = paramValue.getNonNullValue();
        final int value;
        if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).intValue();
        } else if (nonNull instanceof String) {
            value = Integer.parseInt((String) nonNull);
        } else if (nonNull instanceof Long) {
            final long v = (Long) nonNull;
            if (v < minValue || v > maxValue) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = (int) v;
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            value = ((BigInteger) nonNull).intValueExact();
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).intValueExact();
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (value < minValue || value > maxValue) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }


    public static long bindToLong(final int batchIndex, final Value paramValue, final long minValue, final long maxValue)
            throws JdbdException {
        final Object nonNull = paramValue.getNonNullValue();
        final long value;
        if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).longValue();
        } else if (nonNull instanceof String) {
            value = Long.parseLong((String) nonNull);
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            value = ((BigInteger) nonNull).longValueExact();
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).longValueExact();
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (value < minValue || value > maxValue) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }

    public static long bindToLongUnsigned(final int batchIndex, final Value paramValue, final long maxValue)
            throws JdbdException, IllegalArgumentException {
        final Object nonNull = paramValue.getNonNullValue();
        final long value;
        if (nonNull instanceof Integer || nonNull instanceof Long) {
            value = ((Number) nonNull).longValue();
        } else if (nonNull instanceof String) {
            if (JdbdNumbers.isHexNumber((String) nonNull)) {
                value = Long.decode((String) nonNull);
            } else {
                value = Long.parseUnsignedLong((String) nonNull);
            }
        } else if (nonNull instanceof Short) {
            value = ((Short) nonNull) & 0xFFFFL;
        } else if (nonNull instanceof Byte) {
            value = ((Byte) nonNull) & 0xFFFFL;
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            value = Long.parseUnsignedLong(nonNull.toString());
        } else if (nonNull instanceof BigDecimal) {
            value = Long.parseUnsignedLong(((BigDecimal) nonNull).stripTrailingZeros().toPlainString());
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (maxValue != -1L && (value & (~maxValue)) != 0) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }

    public static BigInteger bindToBigInteger(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();

        final BigInteger value;
        if (nonNull instanceof BigInteger) {
            value = (BigInteger) nonNull;
        } else if (nonNull instanceof String) {
            value = new BigInteger((String) nonNull);
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = BigInteger.valueOf(((Number) nonNull).longValue());
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? BigInteger.ONE : BigInteger.ZERO);
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).toBigIntegerExact();
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static BigDecimal bindToDecimal(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();

        final BigDecimal value;
        if (nonNull instanceof BigDecimal) {
            value = (BigDecimal) nonNull;
        } else if (nonNull instanceof String) {
            value = new BigDecimal((String) nonNull);
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = BigDecimal.valueOf(((Number) nonNull).longValue());
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (nonNull instanceof BigInteger) {
            value = new BigDecimal((BigInteger) nonNull);
        } else if (nonNull instanceof Double || nonNull instanceof Float) {
            value = new BigDecimal(nonNull.toString());
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static float bindToFloat(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final float value;
        if (nonNull instanceof Float) {
            value = (Float) nonNull;
        } else if (nonNull instanceof Short || nonNull instanceof Byte) {
            value = ((Number) nonNull).floatValue();
        } else if (nonNull instanceof String) {
            value = Float.parseFloat((String) nonNull);
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0F : 0.0F;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }


    public static double bindToDouble(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final double value;
        if (nonNull instanceof Float
                || nonNull instanceof Double
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).doubleValue();
        } else if (nonNull instanceof String) {
            value = Double.parseDouble((String) nonNull);
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0D : 0.0D;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static String bindToString(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final String value;

        if (nonNull instanceof String) {
            value = (String) nonNull;
        } else if (nonNull instanceof Enum) {
            value = ((Enum<?>) nonNull).name();
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).toPlainString();
        } else if (nonNull instanceof Number
                || nonNull instanceof LocalDate
                || nonNull instanceof UUID
                || nonNull instanceof YearMonth
                || nonNull instanceof MonthDay) {
            value = nonNull.toString();
        } else if (nonNull instanceof LocalDateTime) {
            value = ((LocalDateTime) nonNull).format(JdbdTimes.DATETIME_FORMATTER_6);
        } else if (nonNull instanceof OffsetDateTime || nonNull instanceof ZonedDateTime) {
            value = JdbdTimes.OFFSET_DATETIME_FORMATTER_6.format((TemporalAccessor) nonNull);
        } else if (nonNull instanceof LocalTime) {
            value = JdbdTimes.TIME_FORMATTER_6.format((LocalTime) nonNull);
        } else if (nonNull instanceof OffsetTime) {
            value = JdbdTimes.OFFSET_TIME_FORMATTER_6.format((OffsetTime) nonNull);
        } else if (nonNull instanceof BitSet) {
            value = JdbdStrings.bitSetToBitString((BitSet) nonNull, true);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    /**
     * @return {@link Number} or {@link String}
     */
    public static Object bindToJson(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final Object value;

        if (nonNull instanceof String || nonNull instanceof Number) {
            value = nonNull;
        } else {
            value = bindToString(batchIndex, paramValue);
        }
        return value;
    }


    public static LocalDate bindToLocalDate(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final LocalDate value;
        if (nonNull instanceof LocalDate) {
            value = (LocalDate) nonNull;
        } else if (nonNull instanceof String) {
            value = LocalDate.parse((String) nonNull);
        } else if (nonNull instanceof YearMonth) {
            final YearMonth v = (YearMonth) nonNull;
            value = LocalDate.of(v.getYear(), v.getMonthValue(), 1);
        } else if (nonNull instanceof MonthDay) {
            final MonthDay v = (MonthDay) nonNull;
            value = LocalDate.of(1970, v.getMonthValue(), v.getDayOfMonth());
        } else if (nonNull instanceof Year) {
            value = LocalDate.of(((Year) nonNull).getValue(), 1, 1);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static LocalTime bindToLocalTime(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final LocalTime value;
        if (nonNull instanceof LocalTime) {
            value = (LocalTime) nonNull;
        } else if (nonNull instanceof String) {
            value = LocalTime.parse((String) nonNull, JdbdTimes.TIME_FORMATTER_6);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static LocalDateTime bindToLocalDateTime(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final LocalDateTime value;
        if (nonNull instanceof LocalDateTime) {
            value = (LocalDateTime) nonNull;
        } else if (nonNull instanceof String) {
            value = LocalDateTime.parse((String) nonNull, JdbdTimes.DATETIME_FORMATTER_6);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static OffsetTime bindToOffsetTime(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final OffsetTime value;
        if (nonNull instanceof OffsetTime) {
            value = (OffsetTime) nonNull;
        } else if (nonNull instanceof String) {
            value = OffsetTime.parse((String) nonNull, JdbdTimes.OFFSET_TIME_FORMATTER_6);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }

    public static OffsetDateTime bindToOffsetDateTime(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final OffsetDateTime value;
        if (nonNull instanceof OffsetDateTime) {
            value = (OffsetDateTime) nonNull;
        } else if (nonNull instanceof ZonedDateTime) {
            value = ((ZonedDateTime) nonNull).toOffsetDateTime();
        } else if (nonNull instanceof String) {
            value = OffsetDateTime.parse((String) nonNull, JdbdTimes.OFFSET_DATETIME_FORMATTER_6);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }


    public static String bindToInterval(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final String value;
        if (nonNull instanceof Period) {
            value = nonNull.toString();
        } else if (nonNull instanceof Duration) {
            value = Interval.of((Duration) nonNull).toString(true);
        } else {
            final Interval v;
            if (nonNull instanceof String) {
                v = Interval.parse((String) nonNull);
            } else if (nonNull instanceof Interval) {
                v = (Interval) nonNull;
            } else {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
            }
            value = v.toString(true);
        }
        return value;
    }



    /*################################## blow private method ##################################*/


}
