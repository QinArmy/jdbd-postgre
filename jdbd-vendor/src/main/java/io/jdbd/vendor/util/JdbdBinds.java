package io.jdbd.vendor.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.type.Interval;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamValue;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.*;
import java.util.Comparator;
import java.util.List;

public abstract class JdbdBinds {

    protected JdbdBinds() {
        throw new UnsupportedOperationException();
    }


    public static boolean hasLongData(List<? extends ParamValue> parameterGroup) {
        boolean has = false;
        for (ParamValue bindValue : parameterGroup) {
            if (bindValue.isLongData()) {
                has = true;
                break;
            }
        }
        return has;
    }

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

    public static boolean hasPublisher(ParamBatchStmt<? extends ParamValue> stmt) {
        boolean has = false;
        for (List<? extends ParamValue> group : stmt.getGroupList()) {
            if (hasPublisher(group)) {
                has = true;
                break;
            }
        }
        return has;
    }


    /**
     * <p>
     * Get component class and dimension of array.
     * </p>
     *
     * @param arrayClass class of Array.
     * @return pair, first: component class,second,dimension of array.
     */
    public static Pair<Class<?>, Integer> getArrayDimensions(final Class<?> arrayClass) {
        if (!arrayClass.isArray()) {
            throw new IllegalArgumentException(String.format("%s isn't Array type.", arrayClass.getName()));
        }
        Class<?> componentClass = arrayClass.getComponentType();
        int dimensions = 1;
        while (componentClass.isArray()) {
            dimensions++;
            componentClass = componentClass.getComponentType();

        }
        return new Pair<>(componentClass, dimensions);
    }


    @Nullable
    public static JdbdSQLException sortAndCheckParamGroup(final int groupIndex
            , final List<? extends ParamValue> paramGroup) {

        paramGroup.sort(Comparator.comparingInt(ParamValue::getIndex));

        JdbdSQLException error = null;
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


    public static boolean bindNonNullToBoolean(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final boolean value;
        if (nonNull instanceof Boolean) {
            value = (Boolean) nonNull;
        } else if (nonNull instanceof Integer
                || nonNull instanceof Long
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
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
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            value = BigDecimal.ZERO.compareTo((BigDecimal) nonNull) != 0;
        } else if (nonNull instanceof BigInteger) {
            value = BigInteger.ZERO.compareTo((BigInteger) nonNull) != 0;
        } else if (nonNull instanceof Double
                || nonNull instanceof Float) {
            value = ((Number) nonNull).doubleValue() != 0.0;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }


    public static short bindNonNullToShort(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final short value;

        if (nonNull instanceof String) {
            short s;
            try {
                s = Short.parseShort((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = s;
        } else if (nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).shortValue();
        } else if (nonNull instanceof Integer) {
            final int intValue = (Integer) nonNull;
            if (intValue <= Short.MAX_VALUE && intValue >= Short.MIN_VALUE) {
                value = (short) intValue;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof Long) {
            final long longValue = (Long) nonNull;
            if (longValue <= Short.MAX_VALUE && longValue >= Short.MIN_VALUE) {
                value = (short) longValue;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (short) (v ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            final BigInteger big = (BigInteger) nonNull;
            if (big.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) <= 0
                    && big.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) >= 0) {
                value = big.shortValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) nonNull;
            if (decimal.scale() == 0
                    && decimal.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) <= 0
                    && decimal.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) >= 0) {
                value = decimal.shortValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static int bindNonNullToInt(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {

        final Object nonNull = paramValue.getNonNull();
        final int value;
        if (nonNull instanceof String) {
            int i;
            try {
                i = Integer.parseInt((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = i;
        } else if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).intValue();
        } else if (nonNull instanceof Long) {
            final long longValue = (Long) nonNull;
            if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                value = (int) longValue;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            final BigInteger big = (BigInteger) nonNull;
            if (big.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0
                    && big.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0) {
                value = big.intValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal decimal = (BigDecimal) nonNull;
            if (decimal.scale() == 0
                    && decimal.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0
                    && decimal.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
                value = decimal.intValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static long bindNonNullToLong(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final long value;
        if (nonNull instanceof String) {
            long v;
            try {
                v = Long.parseLong((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = ((Number) nonNull).longValue();
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? 1 : 0);
        } else if (nonNull instanceof BigInteger) {
            final BigInteger big = (BigInteger) nonNull;
            if (big.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0
                    && big.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0) {
                value = big.longValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) nonNull;
            if (decimal.scale() == 0
                    && decimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
                    && decimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
                value = decimal.longValue();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static BigInteger bindToBigInteger(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();

        final BigInteger value;
        if (nonNull instanceof BigInteger) {
            value = (BigInteger) nonNull;
        } else if (nonNull instanceof String) {
            BigInteger v;
            try {
                v = new BigInteger((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = BigInteger.valueOf(((Number) nonNull).longValue());
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? BigInteger.ONE : BigInteger.ZERO);
        } else if (nonNull instanceof BigDecimal) {
            final BigDecimal v = ((BigDecimal) nonNull).stripTrailingZeros();
            if (v.scale() == 0) {
                value = v.toBigInteger();
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static BigDecimal bindNonNullToDecimal(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();

        final BigDecimal value;
        if (nonNull instanceof BigDecimal) {
            value = (BigDecimal) nonNull;
        } else if (nonNull instanceof String) {
            BigDecimal v;
            try {
                v = new BigDecimal((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            value = BigDecimal.valueOf(((Number) nonNull).longValue());
        } else if (nonNull instanceof Boolean) {
            final boolean v = (Boolean) nonNull;
            value = (v ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (nonNull instanceof BigInteger) {
            value = new BigDecimal((BigInteger) nonNull);
        } else if (nonNull instanceof Double
                || nonNull instanceof Float) {
            value = BigDecimal.valueOf(((Number) nonNull).doubleValue());
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static float bindNonNullToFloat(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final float value;
        if (nonNull instanceof Float) {
            value = (Float) nonNull;
        } else if (nonNull instanceof String) {
            float v;
            try {
                v = Float.parseFloat((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0F : 0.0F;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }


    public static double bindNonNullToDouble(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final double value;
        if (nonNull instanceof Float
                || nonNull instanceof Double) {
            value = ((Number) nonNull).doubleValue();
        } else if (nonNull instanceof String) {
            double v;
            try {
                v = Double.parseDouble((String) nonNull);
            } catch (NumberFormatException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else if (nonNull instanceof Boolean) {
            value = ((Boolean) nonNull) ? 1.0D : 0.0D;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static String bindNonNullToString(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final String value;

        if (nonNull instanceof String) {
            value = (String) nonNull;
        } else if (nonNull instanceof Enum) {
            value = ((Enum<?>) nonNull).name();
        } else if (nonNull instanceof BigDecimal) {
            value = ((BigDecimal) nonNull).toPlainString();
        } else if (nonNull instanceof Number) {
            value = nonNull.toString();
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);

        }
        return value;
    }


    public static LocalDate bindNonNullToLocalDate(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final LocalDate value;
        if (nonNull instanceof LocalDate) {
            value = (LocalDate) nonNull;
        } else if (nonNull instanceof String) {
            LocalDate v;
            try {
                v = LocalDate.parse((String) nonNull);
            } catch (DateTimeException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static LocalTime bindNonNullToLocalTime(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final LocalTime value;
        if (nonNull instanceof LocalTime) {
            value = (LocalTime) nonNull;
        } else if (nonNull instanceof String) {
            LocalTime v;
            try {
                v = LocalTime.parse((String) nonNull, JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            } catch (DateTimeException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static LocalDateTime bindNonNullToLocalDateTime(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final LocalDateTime value;
        if (nonNull instanceof LocalDateTime) {
            value = (LocalDateTime) nonNull;
        } else if (nonNull instanceof String) {
            LocalDateTime v;
            try {
                v = LocalDateTime.parse((String) nonNull, JdbdTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static OffsetTime bindNonNullToOffsetTime(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final OffsetTime value;
        if (nonNull instanceof OffsetTime) {
            value = (OffsetTime) nonNull;
        } else if (nonNull instanceof String) {
            OffsetTime v;
            try {
                v = OffsetTime.parse((String) nonNull, JdbdTimes.ISO_OFFSET_TIME_FORMATTER);
            } catch (DateTimeException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static OffsetDateTime bindNonNullToOffsetDateTime(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final OffsetDateTime value;
        if (nonNull instanceof OffsetDateTime) {
            value = (OffsetDateTime) nonNull;
        } else if (nonNull instanceof ZonedDateTime) {
            value = ((ZonedDateTime) nonNull).toOffsetDateTime();
        } else if (nonNull instanceof String) {
            OffsetDateTime v;
            try {
                v = OffsetDateTime.parse((String) nonNull, JdbdTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        return value;
    }


    public static String bindNonNullToInterval(final int batchIndex, SQLType sqlType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final String value;
        if (nonNull instanceof Period) {
            value = nonNull.toString();
        } else if (nonNull instanceof Duration) {
            value = Interval.of((Duration) nonNull).toString(true);
        } else {
            final Interval v;
            if (nonNull instanceof String) {
                try {
                    v = Interval.parse((String) nonNull);
                } catch (DateTimeException e) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
                }
            } else if (nonNull instanceof Interval) {
                v = (Interval) nonNull;
            } else {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
            }
            value = v.toString(true);
        }
        return value;
    }



    /*################################## blow private method ##################################*/


}
