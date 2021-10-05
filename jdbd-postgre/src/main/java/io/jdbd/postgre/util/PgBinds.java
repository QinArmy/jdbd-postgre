package io.jdbd.postgre.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.stmt.UnsupportedBindJavaTypeException;
import io.jdbd.type.Interval;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdArrays;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdExceptions;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Pair;
import org.qinarmy.util.Stack;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

public abstract class PgBinds extends JdbdBinds {


    public static PgType mapJdbcTypeToPgType(final JDBCType jdbcType, @Nullable Object nullable) {
        final PgType pgType;
        switch (Objects.requireNonNull(jdbcType, "jdbcType")) {
            case BIT:
                pgType = PgType.VARBIT;
                break;
            case BOOLEAN:
                pgType = PgType.BOOLEAN;
                break;
            case TINYINT:
            case SMALLINT:
                pgType = PgType.SMALLINT;
                break;
            case INTEGER:
                pgType = PgType.INTEGER;
                break;
            case BIGINT:
            case ROWID:
                pgType = PgType.BIGINT;
                break;
            case TIME:
                pgType = PgType.TIME;
                break;
            case DATE:
                pgType = PgType.DATE;
                break;
            case TIMESTAMP:
                pgType = PgType.TIMESTAMP;
                break;
            case TIME_WITH_TIMEZONE:
                pgType = PgType.TIMETZ;
                break;
            case TIMESTAMP_WITH_TIMEZONE:
                pgType = PgType.TIMESTAMPTZ;
                break;
            case FLOAT:
            case REAL:
                pgType = PgType.REAL;
                break;
            case DOUBLE:
                pgType = PgType.DOUBLE;
                break;
            case NUMERIC:
            case DECIMAL:
                pgType = PgType.DECIMAL;
                break;
            case CHAR:
            case NCHAR:
                pgType = PgType.CHAR;
                break;
            case VARCHAR:
            case NVARCHAR:
                pgType = PgType.VARCHAR;
                break;
            case CLOB:
            case NCLOB:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                pgType = PgType.TEXT;
                break;
            case BINARY:
            case VARBINARY:
            case BLOB:
            case LONGVARBINARY:
                pgType = PgType.BYTEA;
                break;
            case ARRAY:
                pgType = nullable == null ? PgType.UNSPECIFIED : mapPgArrayType(nullable.getClass());
                break;
            case REF: //TODO check this
            case REF_CURSOR:
                pgType = PgType.REF_CURSOR;
                break;
            case SQLXML:
                pgType = PgType.XML;
                break;
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case DATALINK:
            case NULL: {
                pgType = inferPgType(nullable);
            }
            break;
            default:
                throw PgExceptions.createUnexpectedEnumException(jdbcType);
        }
        return pgType;
    }


    public static PgType inferPgType(final @Nullable Object nullable) throws UnsupportedBindJavaTypeException {
        final PgType pgType;
        if (nullable == null) {
            pgType = PgType.UNSPECIFIED;
        } else if (nullable instanceof Number) {
            if (nullable instanceof Integer) {
                pgType = PgType.INTEGER;
            } else if (nullable instanceof Long) {
                pgType = PgType.BIGINT;
            } else if (nullable instanceof Short || nullable instanceof Byte) {
                pgType = PgType.SMALLINT;
            } else if (nullable instanceof Double) {
                pgType = PgType.DOUBLE;
            } else if (nullable instanceof Float) {
                pgType = PgType.REAL;
            } else {
                pgType = PgType.DECIMAL;
            }
        } else if (nullable instanceof String || nullable instanceof Enum) {
            pgType = PgType.VARCHAR;
        } else if (nullable instanceof Boolean) {
            pgType = PgType.BOOLEAN;
        } else if (nullable instanceof Temporal) {
            if (nullable instanceof LocalDateTime) {
                pgType = PgType.TIMESTAMP;
            } else if (nullable instanceof OffsetDateTime || nullable instanceof ZonedDateTime) {
                pgType = PgType.TIMESTAMPTZ;
            } else if (nullable instanceof LocalTime) {
                pgType = PgType.TIME;
            } else if (nullable instanceof OffsetTime) {
                pgType = PgType.TIMETZ;
            } else if (nullable instanceof Instant) {
                pgType = PgType.BIGINT;
            } else if (nullable instanceof LocalDate || nullable instanceof YearMonth || nullable instanceof Year) {
                pgType = PgType.DATE;
            } else {
                throw PgExceptions.notSupportBindJavaType(nullable.getClass());
            }
        } else if (nullable instanceof byte[] || nullable instanceof Path || nullable instanceof Publisher) {
            pgType = PgType.BYTEA;
        } else if (nullable instanceof MonthDay) {
            pgType = PgType.DATE;
        } else if (nullable instanceof Point) {
            pgType = PgType.POINT;
        } else if (nullable instanceof Circle) {
            pgType = PgType.CIRCLES;
        } else if (nullable instanceof BitSet) {
            pgType = PgType.VARBIT;
        } else if (nullable instanceof UUID) {
            pgType = PgType.UUID;
        } else if (nullable instanceof Duration
                || nullable instanceof Period
                || nullable instanceof Interval) {
            pgType = PgType.INTERVAL;
        } else if (nullable.getClass().isArray()) {
            pgType = mapPgArrayType(nullable.getClass());
        } else {
            throw PgExceptions.notSupportBindJavaType(nullable.getClass());
        }
        return pgType;
    }


    public static PgType mapPgArrayType(final Class<?> arrayClass) throws UnsupportedBindJavaTypeException {
        final Pair<Class<?>, Integer> pair;
        pair = JdbdArrays.getArrayDimensions(arrayClass);

        final Class<?> componentClass = pair.getFirst();
        final int dimensions = pair.getSecond();

        final PgType pgType;
        if (componentClass == Integer.class
                || componentClass == int.class) {
            pgType = PgType.INTEGER_ARRAY;
        } else if (componentClass == Long.class
                || componentClass == long.class) {
            pgType = PgType.BIGINT_ARRAY;
        } else if (componentClass == Boolean.class
                || componentClass == boolean.class) {
            pgType = PgType.BOOLEAN_ARRAY;
        } else if (componentClass == Short.class
                || componentClass == Byte.class
                || componentClass == short.class) {
            pgType = PgType.SMALLINT_ARRAY;
        } else if ((componentClass == byte.class)) {
            pgType = dimensions > 1 ? PgType.BYTEA_ARRAY : PgType.BYTEA;
        } else if ((componentClass == Publisher.class)) {
            pgType = PgType.BYTEA_ARRAY;
        } else if (componentClass == Double.class) {
            pgType = PgType.DOUBLE_ARRAY;
        } else if (componentClass == Float.class) {
            pgType = PgType.REAL_ARRAY;
        } else if (componentClass == Character.class
                || componentClass == char.class) {
            pgType = PgType.CHAR_ARRAY;
        } else if (componentClass == String.class
                || componentClass.isEnum()) {
            pgType = PgType.VARCHAR;
        } else if (componentClass == BigDecimal.class
                || componentClass == BigInteger.class) {
            pgType = PgType.DECIMAL_ARRAY;
        } else if (componentClass == LocalDate.class) {
            pgType = PgType.DATE_ARRAY;
        } else if (componentClass == LocalTime.class) {
            pgType = PgType.TIME_ARRAY;
        } else if (componentClass == LocalDateTime.class) {
            pgType = PgType.TIMESTAMP_ARRAY;
        } else if (componentClass == OffsetDateTime.class
                || componentClass == ZonedDateTime.class) {
            pgType = PgType.TIMESTAMPTZ_ARRAY;
        } else if (componentClass == OffsetTime.class) {
            pgType = PgType.TIMETZ_ARRAY;
        } else if (componentClass == Point.class) {
            pgType = PgType.POINT_ARRAY;
        } else if (componentClass == Circle.class) {
            pgType = PgType.CIRCLES_ARRAY;
        } else if (componentClass == UUID.class) {
            pgType = PgType.UUID_ARRAY;
        } else if (componentClass == BitSet.class) {
            pgType = PgType.VARBIT_ARRAY;
        } else if (componentClass == Duration.class
                || componentClass == Period.class
                || componentClass == Interval.class) {
            pgType = PgType.INTERVAL_ARRAY;
        } else {
            throw PgExceptions.notSupportBindJavaType(arrayClass);
        }
        return pgType;
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return value;
    }


    public static int decideFormatCode(final PgType type) {
        final int formatCode;
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case REAL:
            case DOUBLE:
            case OID:
            case BIGINT:
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

    public static String bindNonNullBooleanArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.BOOLEAN_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Boolean.class && arrayType != boolean.class) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, Object::toString);
    }


    public static String bindNonNullDoubleArray(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType != PgType.DOUBLE_ARRAY) {
            throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != Double.class && arrayType != double.class) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                    SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return v;
            };
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                    SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return (String) nonNull;
            };
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                    SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return (String) nonNull;
            };
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            function = nonNull -> String.format("", ((BigDecimal) nonNull).toPlainString());
        } else if (arrayType == String.class || arrayType == Object.class) {
            function = nonNull -> {
                final String result;
                if (nonNull instanceof BigDecimal) {
                    result = ((BigDecimal) nonNull).toPlainString();
                } else if (nonNull instanceof String) {
                    result = textEscapesFunction(nonNull);
                } else {
                    SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
                    throw new JdbdSQLException(e);
                }
                return result;
            };
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
            case LINE_SEGMENT_ARRAY:
            case BOX_ARRAY:
            case PATH_ARRAY:
            case CIRCLES_ARRAY:
            case POLYGON_ARRAY:
                break;
            default:
                throw new IllegalArgumentException("pgType error");
        }
        if (obtainArrayType(batchIndex, pgType, paramValue) != String.class) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final String v = (String) nonNull;
            if (v.indexOf(PgConstant.QUOTE) >= 0
                    || v.indexOf(PgConstant.DOUBLE_QUOTE) >= 0
                    || v.charAt(v.length() - 1) == PgConstant.BACK_SLASH) {
                SQLException e = JdbdExceptions.outOfTypeRange(batchIndex, pgType, paramValue);
                throw new JdbdSQLException(e);
            }
            return PgConstant.DOUBLE_QUOTE + v + PgConstant.DOUBLE_QUOTE;
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Function<Object, String> function = nonNull -> {
            final byte[] v;
            if (nonNull instanceof byte[]) {
                v = (byte[]) nonNull;
            } else if (nonNull instanceof String) {
                v = ((String) nonNull).getBytes(clientCharset);
            } else {
                SQLException e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
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
                break;
            default:
                throw new IllegalArgumentException("pgType error");
        }
        final Class<?> arrayType = obtainArrayType(batchIndex, pgType, paramValue);
        if (arrayType != String.class) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return bindNonNullToArray(batchIndex, pgType, paramValue, PgBinds::textEscapesFunction);
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
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Pair<Class<?>, Integer> pair;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        final Class<?> arrayType = pair.getFirst();
        if (arrayType == byte.class && pair.getSecond() < 2) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        return arrayType;
    }


    private static String bindNonNullToArray(final int batchIndex, PgType pgType, ParamValue paramValue
            , final Function<Object, String> function) throws SQLException {
        if (pgType.jdbcType() != JDBCType.ARRAY) {
            throw new IllegalArgumentException(String.format("pgType[%s] isn't array type", pgType));
        }
        final Object nonNull = paramValue.getNonNull();
        if (!nonNull.getClass().isArray()) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Pair<Class<?>, Integer> pair = JdbdArrays.getArrayDimensions(nonNull.getClass());

        final int topArrayDimension = pair.getSecond();
        final boolean isByteArray;
        if (pair.getFirst() == byte.class) {
            if (topArrayDimension < 2) {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
            }
            isByteArray = true;
        } else {
            isByteArray = false;
        }

        final StringBuilder builder = new StringBuilder();
        final Stack<ArrayWrapper> dimensionStack = new FastStack<>();
        final int topDimensionLength = Array.getLength(nonNull);

        builder.append('{');
        for (int i = 0; i < topDimensionLength; i++) {
            if (i > 0) {
                builder.append(',');
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
                    appendArray(builder, arrayWrapper.array, function);
                    dimensionStack.pop();
                    continue;
                } else if (arrayWrapper.dimension == 1) {
                    appendArray(builder, arrayWrapper.array, function);
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
                    builder.append(',');
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
            , final Function<Object, String> function) {
        final int length = Array.getLength(array);
        builder.append('{');
        Object value;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(',');
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
