package io.jdbd.postgre.util;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.type.Interval;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdArrays;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdExceptions;
import io.qinarmy.util.FastStack;
import io.qinarmy.util.Pair;
import io.qinarmy.util.Stack;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public abstract class PgBinds extends JdbdBinds {


    private PgBinds() {
        throw new UnsupportedOperationException();
    }


    public static String bindNonNullToBit(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final String value;
        if (nonNull instanceof BitSet) {
            value = PgStrings.bitSetToBitString((BitSet) nonNull, false);
        } else if (nonNull instanceof Integer) {
            value = PgStrings.toBinaryString((Integer) nonNull, false);
        } else if (nonNull instanceof Long) {
            value = PgStrings.toBinaryString((Long) nonNull, false);
        } else if (nonNull instanceof String) {
            if (PgStrings.isBinaryString((String) nonNull)) {
                value = (String) nonNull;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, pgType, paramValue);
            }
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return value;
    }

    public static String bindNonNullToVarBit(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final String value;
        if (nonNull instanceof BitSet) {
            value = PgStrings.bitSetToBitString((BitSet) nonNull, false);
        } else if (nonNull instanceof Integer) {
            value = new StringBuilder(Integer.toBinaryString((Integer) nonNull)).reverse().toString();
        } else if (nonNull instanceof Long) {
            value = new StringBuilder(Long.toBinaryString((Long) nonNull)).reverse().toString();
        } else if (nonNull instanceof String) {
            if (PgStrings.isBinaryString((String) nonNull)) {
                value = (String) nonNull;
            } else {
                throw JdbdExceptions.outOfTypeRange(batchIndex, pgType, paramValue);
            }
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return value;
    }


    public static int decideFormatCode(final DataType type) {

        final int formatCode;
        if (!(type instanceof PgType)) {
            formatCode = 0; //  text format
        } else switch ((PgType) type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case FLOAT8:
            case OID:
            case BYTEA:
            case BOOLEAN:
                formatCode = 1; // binary format code
                // only these  is binary format ,because postgre no document about binary format ,and postgre binary protocol not good
                // if change this ,change io.jdbd.postgre.protocol.client.DefaultResultSetReader.parseColumnFromBinary
                break;
            default:
                formatCode = 0; // all array type is text format
        }
        return formatCode;
    }

    public static String bindToPostgreDate(final int batchIndex, final Value paramValue) {
        final Object source = paramValue.getValue();
        String value;
        if (!(source instanceof String)) {
            value = bindToLocalDate(batchIndex, paramValue).toString();
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            value = bindToLocalDate(batchIndex, paramValue).toString();
        }
        return value;
    }

    public static String bindToPostgreTimestamp(final int batchIndex, final Value paramValue) {
        final Object source = paramValue.getValue();
        String value;
        if (!(source instanceof String)) {
            value = bindToLocalDateTime(batchIndex, paramValue).format(PgTimes.DATETIME_FORMATTER_6);
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            value = bindToLocalDateTime(batchIndex, paramValue).format(PgTimes.DATETIME_FORMATTER_6);
        }
        return value;
    }

    public static String bindToPostgreTimestampTz(final int batchIndex, final Value paramValue) {
        final Object source = paramValue.getValue();
        String value;
        if (!(source instanceof String)) {
            value = bindToOffsetDateTime(batchIndex, paramValue).format(PgTimes.OFFSET_DATETIME_FORMATTER_6);
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) source)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            value = bindToOffsetDateTime(batchIndex, paramValue).format(PgTimes.OFFSET_DATETIME_FORMATTER_6);
        }
        return value;
    }

    public static String bindToBooleanArray(final int batchIndex, final ParamValue paramValue) throws JdbdException {
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Boolean.class && arrayType != boolean.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullSmallIntArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.SMALLINT_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Short.class && arrayType != short.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullIntegerArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.INTEGER_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Integer.class && arrayType != int.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullBigIntArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.BIGINT_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Long.class && arrayType != long.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullFloatArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.REAL_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Float.class && arrayType != float.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }


    public static String bindNonNullDoubleArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.FLOAT8_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Double.class && arrayType != double.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullDecimalArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.DECIMAL_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == BigDecimal.class) {
            function = v -> ((BigDecimal) v).toPlainString();
        } else if (arrayType == BigInteger.class) {
            function = Object::toString;
        } else if (arrayType == Object.class) {
            function = nonNull -> {
                final String v;
                if (nonNull instanceof BigDecimal) {
                    v = ((BigDecimal) nonNull).toPlainString();
                } else if (nonNull instanceof String && PgConstant.NaN.equalsIgnoreCase((String) nonNull)) {
                    v = (String) nonNull;
                } else {
                    SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return v;
            };
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }


    public static String bindNonNullBitArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.BIT_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == BitSet.class) {
            function = nonNull -> PgStrings.bitSetToBitString((BitSet) nonNull, false);
        } else if (arrayType == Long.class || arrayType == long.class) {
            function = nonNull -> PgStrings.toBinaryString((Long) nonNull, false);
        } else if (arrayType == Integer.class || arrayType == int.class) {
            function = nonNull -> PgStrings.toBinaryString((Integer) nonNull, false);
        } else if (arrayType == String.class) {
            function = nonNull -> {
                if (!PgStrings.isBinaryString((String) nonNull)) {
                    SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return (String) nonNull;
            };
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullVarBitArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.VARBIT_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == BitSet.class) {
            function = nonNull -> PgStrings.bitSetToBitString((BitSet) nonNull, false);
        } else if (arrayType == Long.class || arrayType == long.class) {
            function = nonNull -> PgStrings.reverse(Long.toBinaryString((Long) nonNull));
        } else if (arrayType == Integer.class || arrayType == int.class) {
            function = nonNull -> PgStrings.reverse(Integer.toBinaryString((Integer) nonNull));
        } else if (arrayType == String.class) {
            function = nonNull -> {
                if (!PgStrings.isBinaryString((String) nonNull)) {
                    SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return (String) nonNull;
            };
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullTimeArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.TIME_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == LocalTime.class) {
            function = nonNull -> ((LocalTime) nonNull).format(PgTimes.ISO_LOCAL_TIME_FORMATTER);
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }


    public static String bindNonNullDateArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.DATE_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != LocalDate.class && arrayType != Object.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final String value;
            if (nonNull instanceof LocalDate) {
                value = ((LocalDate) nonNull).format(PgTimes.PG_ISO_LOCAL_DATE_FORMATTER);
            } else if (nonNull instanceof String
                    && (PgConstant.INFINITY.equalsIgnoreCase((String) nonNull)
                    || PgConstant.NEG_INFINITY.equalsIgnoreCase((String) nonNull))) {
                value = (String) nonNull;
            } else {
                SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                throw new JdbdSQLException(e);
            }
            return value;
        };
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullTimestampArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.TIMESTAMP_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);

        if (arrayType != LocalDateTime.class && arrayType != Object.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final String value;
            if (nonNull instanceof LocalDateTime) {
                value = ((LocalDateTime) nonNull).format(PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER);
            } else if (nonNull instanceof String
                    && (PgConstant.INFINITY.equalsIgnoreCase((String) nonNull)
                    || PgConstant.NEG_INFINITY.equalsIgnoreCase((String) nonNull))) {
                value = (String) nonNull;
            } else {
                SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                throw new JdbdSQLException(e);
            }
            return value;
        };
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullTimeTzArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.TIMETZ_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == OffsetTime.class) {
            function = nonNull -> ((OffsetTime) nonNull).format(PgTimes.ISO_OFFSET_TIME_FORMATTER);
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullTimestampTzArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.TIMESTAMPTZ_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != OffsetDateTime.class && arrayType != ZonedDateTime.class && arrayType != Object.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final String value;
            if (nonNull instanceof OffsetDateTime) {
                value = ((OffsetDateTime) nonNull).format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER);
            } else if (nonNull instanceof ZonedDateTime) {
                value = ((ZonedDateTime) nonNull).format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER);
            } else if (nonNull instanceof String
                    && (PgConstant.INFINITY.equalsIgnoreCase((String) nonNull)
                    || PgConstant.NEG_INFINITY.equalsIgnoreCase((String) nonNull))) {
                value = (String) nonNull;
            } else {
                SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                throw new JdbdSQLException(e);
            }
            return value;
        };
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullUuidArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.UUID_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != UUID.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }

    public static String bindNonNullIntervalArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.INTERVAL_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == Interval.class) {
            function = nonNull -> ((Interval) nonNull).toString(true);
        } else if (arrayType == Duration.class) {
            function = nonNull -> Interval.of((Duration) nonNull).toString(true);
        } else if (arrayType == Period.class) {
            function = nonNull -> Interval.of((Period) nonNull).toString(true);
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullMoneyArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.MONEY_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == BigDecimal.class) {
            function = nonNull -> ((BigDecimal) nonNull).toPlainString();
        } else if (arrayType == String.class) {
            function = PgBinds::textEscapesFunction;
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }


    public static String bindNonNullSafeTextArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException, JdbdSQLException {
        switch (pgType) {
            case NUMRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case TSRANGE_ARRAY:

            case INET_ARRAY:
            case CIDR_ARRAY:
            case MACADDR8_ARRAY:
            case MACADDR_ARRAY:

            case POINT_ARRAY:
            case LINE_ARRAY:
            case LSEG_ARRAY:
            case BOX_ARRAY:
            case PATH_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY:
                break;
            default:
                throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != String.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            for (char c : ((String) nonNull).toCharArray()) {
                switch (c) {
                    case PgConstant.BACK_SLASH:
                    case PgConstant.DOUBLE_QUOTE:
                        SQLException e;
                        e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                        throw new JdbdSQLException(e);
                    default:
                        //no-op
                }
            }
            return PgConstant.DOUBLE_QUOTE + ((String) nonNull) + PgConstant.DOUBLE_QUOTE;
        };

        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullByteaArray(final int batchIndex, PgType pgType, ParamValue paramValue
            , final Charset clientCharset) throws SQLException {
        if (pgType != PgType.BYTEA_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != byte.class && arrayType != String.class) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final byte[] v;
            if (nonNull instanceof byte[]) {
                v = (byte[]) nonNull;
            } else if (nonNull instanceof String) {
                v = ((String) nonNull).getBytes(clientCharset);
            } else {
                SQLException e = JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                throw new JdbdSQLException(e);
            }
            @SuppressWarnings("all")
            String result = new StringBuilder(4 + (v.length << 1))
                    .append(PgConstant.DOUBLE_QUOTE)
                    .append(PgConstant.BACK_SLASH)
                    .append(PgConstant.BACK_SLASH)
                    .append('x')
                    .append(PgBuffers.hexEscapesText(true, v, v.length))
                    .append(PgConstant.DOUBLE_QUOTE)
                    .toString();
            return result;
        };
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }

    public static String bindNonNullEscapesTextArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException, JdbdSQLException {
        switch (pgType) {
            case TSVECTOR_ARRAY:
            case TSQUERY_ARRAY:
            case TEXT_ARRAY:
            case XML_ARRAY:
            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
            case UNSPECIFIED:// custom type
                break;
            default:
                throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        final Function<Object, String> function;
        if (arrayType == String.class) {
            function = PgBinds::textEscapesFunction;
        } else if (arrayType.isEnum()) {
            function = nonNull -> ((Enum<?>) nonNull).name();
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, function);
    }


    public static Map<String, PgType> createPgTypeMap() {
        final PgType[] pgTypeArray = PgType.values();
        final Map<String, PgType> map = PgCollections.hashMap((int) ((pgTypeArray.length + 48) / 0.75f));

        for (final PgType type : pgTypeArray) {
            switch (type) {
                case REF_CURSOR:
                case REF_CURSOR_ARRAY:
                case UNSPECIFIED:
                    continue;
                default:
                    //no-op
            }

            if (map.putIfAbsent(type.typeName(), type) != null) {
                // no bug,never here
                String m = String.format("type[%s] error", type.name());
                throw new IllegalStateException(m);
            }

        }

        //add alias pairs

        map.putIfAbsent("BOOL", PgType.BOOLEAN);
        map.putIfAbsent("INT2", PgType.SMALLINT);
        map.putIfAbsent("SMALLSERIAL", PgType.SMALLINT);
        map.putIfAbsent("INT", PgType.INTEGER);

        map.putIfAbsent("INT4", PgType.INTEGER);
        map.putIfAbsent("SERIAL", PgType.INTEGER);
        map.putIfAbsent("INT8", PgType.BIGINT);
        map.putIfAbsent("BIGINT", PgType.BIGINT);

        map.putIfAbsent("BIGSERIAL", PgType.BIGINT);
        map.putIfAbsent("SERIAL8", PgType.BIGINT);
        map.putIfAbsent("NUMERIC", PgType.DECIMAL);
        map.putIfAbsent("DECIMAL", PgType.DECIMAL);

        map.putIfAbsent("FLOAT8", PgType.FLOAT8);
        map.putIfAbsent("DOUBLE PRECISION", PgType.FLOAT8);
        map.putIfAbsent("FLOAT", PgType.FLOAT8);
        map.putIfAbsent("FLOAT4", PgType.REAL);

        map.putIfAbsent("REAL", PgType.REAL);
        map.putIfAbsent("CHAR", PgType.CHAR);
        map.putIfAbsent("CHARACTER", PgType.CHAR);
        map.putIfAbsent("VARCHAR", PgType.VARCHAR);

        map.putIfAbsent("CHARACTER VARYING", PgType.VARCHAR);
        map.putIfAbsent("TIME WITHOUT TIME ZONE", PgType.TIME);
        map.putIfAbsent("TIME WITH TIME ZONE", PgType.TIMETZ);
        map.putIfAbsent("TIMESTAMP WITHOUT TIME ZONE", PgType.TIMESTAMP);

        map.putIfAbsent("TIMESTAMP WITH TIME ZONE", PgType.TIMESTAMPTZ);
        map.putIfAbsent("BIT VARYING", PgType.VARBIT);
        map.putIfAbsent("BOOL[]", PgType.BOOLEAN_ARRAY);
        map.putIfAbsent("INT2[]", PgType.SMALLINT_ARRAY);

        map.putIfAbsent("SMALLSERIAL[]", PgType.SMALLINT_ARRAY);
        map.putIfAbsent("INT[]", PgType.INTEGER_ARRAY);
        map.putIfAbsent("INT4[]", PgType.INTEGER_ARRAY);
        map.putIfAbsent("SERIAL[]", PgType.INTEGER_ARRAY);

        map.putIfAbsent("INT8[]", PgType.BIGINT_ARRAY);
        map.putIfAbsent("SERIAL8[]", PgType.BIGINT_ARRAY);
        map.putIfAbsent("BIGSERIAL[]", PgType.BIGINT_ARRAY);
        map.putIfAbsent("NUMERIC[]", PgType.DECIMAL_ARRAY);

        map.putIfAbsent("FLOAT[]", PgType.FLOAT8_ARRAY);
        map.putIfAbsent("DOUBLE PRECISION[]", PgType.FLOAT8_ARRAY);
        map.putIfAbsent("FLOAT4[]", PgType.REAL_ARRAY);
        map.putIfAbsent("CHARACTER[]", PgType.CHAR_ARRAY);

        map.putIfAbsent("CHARACTER VARYING[]", PgType.VARCHAR_ARRAY);
        map.putIfAbsent("TIME WITHOUT TIME ZONE[]", PgType.TIME_ARRAY);
        map.putIfAbsent("TIME WITH TIME ZONE[]", PgType.TIMETZ_ARRAY);

        map.putIfAbsent("TIMESTAMP WITHOUT TIME ZONE[]", PgType.TIMESTAMP_ARRAY);
        map.putIfAbsent("TIMESTAMP WITH TIME ZONE[]", PgType.TIMESTAMPTZ_ARRAY);
        map.putIfAbsent("BIT VARYING[]", PgType.VARBIT_ARRAY);

        return PgCollections.unmodifiableMap(map);
    }


    private static String textEscapesFunction(final Object nonNull) {
        final char[] charArray = ((String) nonNull).toCharArray();
        int lastWritten = 0;
        char c;
        final StringBuilder builder = new StringBuilder(charArray.length + 10);
        builder.append(PgConstant.DOUBLE_QUOTE);

        for (int i = 0; i < charArray.length; i++) {
            c = charArray[i];
            switch (c) {
                case PgConstant.BACK_SLASH:
                case PgConstant.DOUBLE_QUOTE: {
                    if (i > lastWritten) {
                        builder.append(charArray, lastWritten, i - lastWritten);
                    }
                    builder.append(PgConstant.BACK_SLASH);
                    lastWritten = i;
                }
                break;
                default:
                    //no-op
            }

        }
        if (lastWritten < charArray.length) {
            builder.append(charArray, lastWritten, charArray.length - lastWritten);
        }
        return builder
                .append(PgConstant.DOUBLE_QUOTE)
                .toString();
    }


    private static Class<?> obtainArrayType(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        final Class<?> arrayClass = paramValue.getNonNull().getClass();
        if (!arrayClass.isArray()) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Pair<Class<?>, Integer> pair;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        final Class<?> arrayType = pair.getFirst();
        if (arrayType == byte.class && pair.getSecond() < 2) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return arrayType;
    }


    private static String bindNonNullToArray(final int batchIndex, PgType pgType, ParamValue paramValue
            , final Function<Object, String> function) throws SQLException {
        if (!pgType.isArray() && pgType != PgType.UNSPECIFIED) {
            throw new IllegalArgumentException(String.format("pgType[%s] isn't array type", pgType));
        }
        final Object nonNull = paramValue.getNonNull();
        if (!nonNull.getClass().isArray()) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Pair<Class<?>, Integer> pair = JdbdArrays.getArrayDimensions(nonNull.getClass());

        final int topArrayDimension = pair.getSecond();
        final boolean isByteArray;
        if (pair.getFirst() == byte.class) {
            if (topArrayDimension < 2) {
                throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
            }
            isByteArray = true;
        } else {
            isByteArray = false;
        }

        final StringBuilder builder = new StringBuilder();
        final Stack<ArrayWrapper> dimensionStack = new FastStack<>();
        final int topDimensionLength = Array.getLength(nonNull);
        final char delim = pgType == PgType.BOX_ARRAY ? ';' : ',';

        builder.append('{');
        for (int i = 0; i < topDimensionLength; i++) {
            if (i > 0) {
                builder.append(delim);
            }
            final Object topValue = Array.get(nonNull, i);
            if (topValue == null) {
                builder.append(PgConstant.NULL);
                continue;
            } else if (isByteArray && topArrayDimension == 2) {
                builder.append(function.apply(topValue));
                continue;
            } else if (topArrayDimension == 1) {
                builder.append(function.apply(topValue));
                continue;
            }
            dimensionStack.push(new ArrayWrapper(topValue, topArrayDimension - 1));
            while (!dimensionStack.isEmpty()) {
                final ArrayWrapper arrayWrapper = dimensionStack.peek();
                if (isByteArray && arrayWrapper.dimension == 2) {
                    appendArray(builder, arrayWrapper.array, delim, function);
                    dimensionStack.pop();
                    continue;
                } else if (arrayWrapper.dimension == 1) {
                    appendArray(builder, arrayWrapper.array, delim, function);
                    dimensionStack.pop();
                    continue;
                }
                final int length = arrayWrapper.length;
                if (length == 0) {
                    builder.append('{')
                            .append('}');
                    dimensionStack.pop();
                    continue;
                }
                final int index = arrayWrapper.index;
                if (index == 0) {
                    builder.append('{');
                } else if (index < length) {
                    builder.append(delim);
                } else {
                    builder.append('}');
                    dimensionStack.pop();
                    continue;
                }
                final Object array = Array.get(arrayWrapper.array, index);
                if (array == null) {
                    builder.append(PgConstant.NULL);
                } else {
                    dimensionStack.push(new ArrayWrapper(array, arrayWrapper.dimension - 1));
                }
                arrayWrapper.index++;

            }

        }
        builder.append('}');
        return builder.toString();
    }


    /**
     * @see #bindNonNullToArray(int, PgType, ParamValue, Function)
     */
    private static void appendArray(final StringBuilder builder, final Object array
            , final char delim, final Function<Object, String> function) {
        final int length = Array.getLength(array);
        builder.append('{');
        Object value;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(delim);
            }
            value = Array.get(array, i);
            if (value == null) {
                builder.append(PgConstant.NULL);
            } else {
                builder.append(function.apply(value));
            }
        }
        builder.append('}');
    }


    private static final class ArrayWrapper {

        private final Object array;

        private final int length;

        private final int dimension;

        private int index = 0;

        private ArrayWrapper(Object array, int dimension) {
            this.array = array;
            this.length = Array.getLength(array);
            this.dimension = dimension;
        }

    }


}
