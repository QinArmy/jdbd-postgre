package io.jdbd.postgre.util;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.stmt.UnsupportedBindJavaTypeException;
import io.jdbd.type.Interval;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.stmt.ParamValue;
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
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.BitSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public abstract class PgBinds extends JdbdBinds {


    private static final Set<Class<?>> ARRAY_CLASS_WITHOUT_ESCAPES_SET = PgArrays.asUnmodifiableSet(
            String.class,
            Boolean.class,
            boolean.class,
            Byte.class,// no byte.class
            Short.class,
            short.class,
            Integer.class,
            int.class,
            Long.class,
            long.class,
            Float.class,
            float.class,
            Double.class,
            double.class,
            BigDecimal.class,
            BigInteger.class,
            UUID.class,
            LocalDate.class,
            Interval.class,
            Duration.class,
            Period.class
    );


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
            pgType = PgType.CIRCLE;
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
        pair = getArrayDimensions(arrayClass);

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
            final char[] bitChars = new char[32];
            final int v = (Integer) nonNull;
            int site = 1;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((v & site) == 0) ? '0' : '1';
                site <<= 1;
            }
            value = new String(bitChars);
        } else if (nonNull instanceof Long) {
            final char[] bitChars = new char[64];
            final long v = (Long) nonNull;
            long site = 1;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((v & site) == 0) ? '0' : '1';
                site <<= 1;
            }
            value = new String(bitChars);
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


    public static String bindNonNullToArrayWithoutEscapes(final int batchIndex, PgType pgType, ParamValue paramValue)
            throws SQLException {
        if (pgType.jdbcType() != JDBCType.ARRAY) {
            throw new IllegalArgumentException(String.format("pgType[%s] isn't array type", pgType));
        }
        final Object nonNull = paramValue.getNonNull();
        if (!nonNull.getClass().isArray()) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }
        final Pair<Class<?>, Integer> pair = getArrayDimensions(nonNull.getClass());
        if (!ARRAY_CLASS_WITHOUT_ESCAPES_SET.contains(pair.getFirst())) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, pgType, paramValue);
        }

        final int topArrayDimension = pair.getSecond();
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
            } else if (topArrayDimension == 1) {
                builder.append(topValue);
                continue;
            }
            dimensionStack.push(new ArrayWrapper(topValue, topArrayDimension - 1));
            while (!dimensionStack.isEmpty()) {
                final ArrayWrapper arrayWrapper = dimensionStack.peek();
                if (arrayWrapper.dimension == 1) {
                    appendArray(builder, arrayWrapper.array);
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
     * @see #bindNonNullToArrayWithoutEscapes(int, PgType, ParamValue)
     */
    private static void appendArray(final StringBuilder builder, final Object array) {
        final int length = Array.getLength(array);
        builder.append('{');
        Object value;
        for (int j = 0; j < length; j++) {
            if (j > 0) {
                builder.append(',');
            }
            value = Array.get(array, j);
            if (value == null) {
                builder.append(PgConstant.NULL);
            } else {
                builder.append(value);
            }
        }
        builder.append('}');
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
