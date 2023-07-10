package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdNumbers;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class ColumnConverts {

    private ColumnConverts() {
        throw new UnsupportedOperationException();
    }


    public static boolean convertToBoolean(final ColumnMeta meta, final Object source) throws JdbdException {
        final boolean value;

        if (source instanceof Boolean) {
            value = (Boolean) source;
        } else if (source instanceof Integer
                || source instanceof Short
                || source instanceof Byte) {
            value = ((Number) source).intValue() != 0;
        } else if (source instanceof Long) {
            value = ((Number) source).longValue() != 0;
        } else if (source instanceof String) {
            final String v = (String) source;
            if (v.equalsIgnoreCase("TRUE")
                    || v.equalsIgnoreCase("T")) {
                value = true;
            } else if (v.equalsIgnoreCase("FALSE")
                    || v.equalsIgnoreCase("F")) {
                value = false;
            } else {
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, Boolean.class, null);
            }
        } else if (source instanceof BigDecimal) {
            value = BigDecimal.ZERO.compareTo((BigDecimal) source) != 0;
        } else if (source instanceof BigInteger) {
            value = BigInteger.ZERO.compareTo((BigInteger) source) != 0;
        } else if (source instanceof Double
                || source instanceof Float) {
            value = ((Number) source).doubleValue() != 0.0;
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, Boolean.class, null);
        }
        return value;
    }

    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static byte convertToByte(final ColumnMeta meta, final Object source) throws JdbdException {
        final int value;
        if (meta.isUnsigned()) {
            value = convertToIntUnsignedValue(meta, source, Byte.class, 0xFF);
        } else {
            value = convertToIntValue(meta, source, Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return (byte) value;
    }

    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static short convertToShot(final ColumnMeta meta, final Object source) throws JdbdException {
        final int value;
        if (meta.isUnsigned()) {
            value = convertToIntUnsignedValue(meta, source, Short.class, 0xFFFF);
        } else {
            value = convertToIntValue(meta, source, Short.class, Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return (short) value;
    }

    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static int convertToInt(final ColumnMeta meta, final Object source) throws JdbdException {
        final int value;
        if (meta.isUnsigned()) {
            value = convertToIntUnsignedValue(meta, source, Integer.class, -1);
        } else {
            value = convertToIntValue(meta, source, Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return value;
    }

    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static long convertToLong(final ColumnMeta meta, final Object source) throws JdbdException {
        final long value;
        if (meta.isUnsigned()) {
            value = convertToLongUnsignedValue(meta, source, Long.class, -1L);
        } else {
            value = convertToLongValue(meta, source, Long.class, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return value;
    }


    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static int convertToIntValue(final ColumnMeta meta, final Object source, final Class<?> targetClass,
                                        final int minValue, final int maxValue) {
        final int value;
        if (source instanceof Integer
                || source instanceof Short
                || source instanceof Byte) {
            value = ((Number) source).intValue();
        } else if (source instanceof String) {
            value = Integer.parseInt((String) source);
        } else if (source instanceof Long) {
            final long v = (Long) source;
            if (v < minValue || v > maxValue) {
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
            }
            value = (int) v;
        } else if (source instanceof Boolean) {
            value = ((Boolean) source ? 1 : 0);
        } else if (source instanceof BigInteger) {
            value = ((BigInteger) source).intValueExact();
        } else if (source instanceof BigDecimal) {
            value = ((BigDecimal) source).intValueExact();
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
        }

        if (value < minValue || value > maxValue) {
            throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
        }
        return value;
    }


    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static int convertToIntUnsignedValue(final ColumnMeta meta, final Object source, final Class<?> targetClass,
                                                final int maxValue) {

        final int value;
        if (source instanceof Integer) {
            value = (Integer) source;
        } else if (source instanceof Long) {
            final long v = (Long) source;
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
            }
            value = (int) v;
        } else if (source instanceof String) {
            if (JdbdNumbers.isHexNumber((String) source)) {
                value = Integer.decode((String) source);
            } else {
                value = Integer.parseUnsignedInt((String) source);
            }
        } else if (source instanceof Short) {
            value = ((Short) source) & 0xFFFF;
        } else if (source instanceof Byte) {
            value = ((Byte) source) & 0xFFFF;
        } else if (source instanceof Boolean) {
            value = ((Boolean) source ? 1 : 0);
        } else if (source instanceof BigInteger) {
            final long v = ((BigInteger) source).longValueExact();
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
            }
            value = (int) v;
        } else if (source instanceof BigDecimal) {
            final long v = ((BigDecimal) source).longValueExact();
            if ((v & (~0xFFFF_FFFFL)) != 0) {
                throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
            }
            value = (int) v;
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
        }

        if (maxValue != -1 && (value & (~maxValue)) != 0) {
            throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
        }
        return value;

    }


    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static long convertToLongValue(final ColumnMeta meta, final Object source, final Class<?> targetClass,
                                          final long minValue, final long maxValue) {
        final long value;
        if (source instanceof Long
                || source instanceof Integer
                || source instanceof Short
                || source instanceof Byte) {
            value = ((Number) source).longValue();
        } else if (source instanceof String) {
            value = Long.parseLong((String) source);
        } else if (source instanceof Boolean) {
            final boolean v = (Boolean) source;
            value = (v ? 1 : 0);
        } else if (source instanceof BigInteger) {
            value = ((BigInteger) source).longValueExact();
        } else if (source instanceof BigDecimal) {
            value = ((BigDecimal) source).longValueExact();
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
        }

        if (value < minValue || value > maxValue) {
            throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
        }
        return value;
    }

    /**
     * @throws NumberFormatException throw when source is error number {@link String}
     * @throws ArithmeticException   throw when source overflow to target
     * @throws JdbdException         throw when source couldn't convert or overflow.
     */
    public static long convertToLongUnsignedValue(final ColumnMeta meta, final Object source, final Class<?> targetClass,
                                                  final long maxValue) {
        final long value;
        if (source instanceof Integer || source instanceof Long) {
            value = ((Number) source).longValue();
        } else if (source instanceof String) {
            if (JdbdNumbers.isHexNumber((String) source)) {
                value = Long.decode((String) source);
            } else {
                value = Long.parseUnsignedLong((String) source);
            }
        } else if (source instanceof Short) {
            value = ((Short) source) & 0xFFFFL;
        } else if (source instanceof Byte) {
            value = ((Byte) source) & 0xFFFFL;
        } else if (source instanceof Boolean) {
            value = ((Boolean) source ? 1 : 0);
        } else if (source instanceof BigInteger) {
            value = Long.parseUnsignedLong(source.toString());
        } else if (source instanceof BigDecimal) {
            value = Long.parseUnsignedLong(((BigDecimal) source).stripTrailingZeros().toPlainString());
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, targetClass, null);
        }

        if (maxValue != -1L && (value & (~maxValue)) != 0) {
            throw JdbdExceptions.columnValueOverflow(meta, source, targetClass, null);
        }
        return value;
    }


}
