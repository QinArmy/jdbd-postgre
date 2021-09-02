package io.jdbd.postgre.util;

import io.jdbd.postgre.PgType;
import io.jdbd.stmt.UnsupportedBindJavaTypeException;
import io.jdbd.type.IntervalPair;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.JdbdBinds;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;

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
            pgType = PgType.CIRCLE;
        } else if (nullable instanceof BitSet) {
            pgType = PgType.VARBIT;
        } else if (nullable instanceof UUID) {
            pgType = PgType.UUID;
        } else if (nullable instanceof Duration
                || nullable instanceof Period
                || nullable instanceof IntervalPair) {
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
                || componentClass == IntervalPair.class) {
            pgType = PgType.INTERVAL_ARRAY;
        } else {
            throw PgExceptions.notSupportBindJavaType(arrayClass);
        }
        return pgType;
    }


}
