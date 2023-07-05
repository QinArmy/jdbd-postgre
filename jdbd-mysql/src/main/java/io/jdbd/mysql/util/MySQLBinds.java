package io.jdbd.mysql.util;

import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.statement.UnsupportedBindJavaTypeException;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.*;

public abstract class MySQLBinds extends JdbdBinds {

    public static MySQLType mapJdbcTypeToMySQLType(final JDBCType jdbcType, @Nullable Object nullable) {
        final MySQLType type;
        switch (Objects.requireNonNull(jdbcType, "jdbcType")) {
            case TINYINT: {
                type = (nullable == null || nullable instanceof Byte)
                        ? MySQLType.TINYINT : MySQLType.TINYINT_UNSIGNED;
            }
            break;
            case SMALLINT: {
                if (nullable instanceof Year) {
                    type = MySQLType.YEAR;
                } else if (nullable == null
                        || nullable instanceof Short
                        || nullable instanceof Byte) {
                    type = MySQLType.SMALLINT;
                } else {
                    type = MySQLType.SMALLINT_UNSIGNED;
                }
            }
            break;
            case INTEGER: {
                if (nullable instanceof Year) {
                    type = MySQLType.YEAR;
                } else if (nullable == null
                        || nullable instanceof Integer
                        || nullable instanceof Short
                        || nullable instanceof Byte) {
                    type = MySQLType.INT;
                } else {
                    type = MySQLType.INT_UNSIGNED;
                }
            }
            break;
            case BIGINT: {
                if (nullable == null
                        || nullable instanceof Long
                        || nullable instanceof Integer
                        || nullable instanceof Short
                        || nullable instanceof Byte) {
                    type = MySQLType.BIGINT;
                } else {
                    type = MySQLType.BIGINT_UNSIGNED;
                }
            }
            break;
            case DECIMAL:
            case NUMERIC:
                type = MySQLType.DECIMAL;
                break;
            case FLOAT:
                type = MySQLType.FLOAT;
                break;
            case DOUBLE:
            case REAL:
                type = MySQLType.DOUBLE;
                break;
            case BIT:
                type = MySQLType.BIT;
                break;
            case BOOLEAN:
                type = MySQLType.BOOLEAN;
                break;
            case CHAR:
            case NCHAR:
                type = MySQLType.CHAR;
                break;
            case VARCHAR:
            case NVARCHAR:
                type = MySQLType.VARCHAR;
                break;
            case TIME:
                type = MySQLType.TIME;
                break;
            case DATE: {
                if (nullable instanceof Year) {
                    type = MySQLType.YEAR;
                } else {
                    type = MySQLType.DATE;
                }
            }
            break;
            case TIMESTAMP:
                type = MySQLType.DATETIME;
                break;
            case BINARY:
                type = MySQLType.BINARY;
                break;
            case VARBINARY:
                type = MySQLType.VARBINARY;
                break;
            case NCLOB:
            case CLOB:
                type = MySQLType.MEDIUMTEXT;
                break;
            case BLOB:
                type = MySQLType.MEDIUMBLOB;
                break;
            case LONGVARCHAR:
            case LONGNVARCHAR:
                type = MySQLType.LONGTEXT;
                break;
            case LONGVARBINARY:
                type = MySQLType.LONGBLOB;
                break;
            case SQLXML:
                type = MySQLType.JSON;
                break;
            case OTHER:
                type = inferMySQLType(nullable);
                break;
            case NULL:
            case ARRAY:
            case ROWID:
            case STRUCT:
            case DATALINK:
            case DISTINCT:
            case REF:
            case REF_CURSOR:
            case JAVA_OBJECT:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
            default:
                throw MySQLExceptions.createUnexpectedEnumException(jdbcType);
        }
        return type;
    }

    public static MySQLType inferMySQLType(final @Nullable Object nullable) throws UnsupportedBindJavaTypeException {
        final MySQLType type;
        if (nullable == null) {
            type = MySQLType.NULL;
        } else if (nullable instanceof Number) {
            if (nullable instanceof Integer) {
                type = MySQLType.INT;
            } else if (nullable instanceof Long) {
                type = MySQLType.BIGINT;
            } else if (nullable instanceof Short) {
                type = MySQLType.SMALLINT;
            } else if (nullable instanceof Byte) {
                type = MySQLType.TINYINT;
            } else if (nullable instanceof Double) {
                type = MySQLType.DOUBLE;
            } else if (nullable instanceof Float) {
                type = MySQLType.FLOAT;
            } else {
                type = MySQLType.DECIMAL;
            }
        } else if (nullable instanceof Enum) {
            type = MySQLType.ENUM;
        } else if (nullable instanceof String || nullable instanceof UUID) {
            type = MySQLType.VARCHAR;
        } else if (nullable instanceof Boolean) {
            type = MySQLType.BOOLEAN;
        } else if (nullable instanceof Temporal) {
            if (nullable instanceof LocalDateTime
                    || nullable instanceof OffsetDateTime
                    || nullable instanceof ZonedDateTime) {
                type = MySQLType.DATETIME;
            } else if (nullable instanceof LocalTime) {
                type = MySQLType.TIME;
            } else if (nullable instanceof Year) {
                type = MySQLType.YEAR;
            } else if (nullable instanceof Instant) {
                type = MySQLType.BIGINT;
            } else if (nullable instanceof LocalDate || nullable instanceof YearMonth) {
                type = MySQLType.DATE;
            } else {
                throw MySQLExceptions.notSupportBindJavaType(nullable.getClass());
            }
        } else if (nullable instanceof byte[]) {
            type = MySQLType.VARBINARY;
        } else if (nullable instanceof Path || nullable instanceof Publisher) {
            type = MySQLType.LONGBLOB;
        } else if (nullable instanceof MonthDay) {
            type = MySQLType.DATE;
        } else if (nullable instanceof BitSet) {
            type = MySQLType.BIT;
        } else if (nullable instanceof Duration) {
            type = MySQLType.TIME;
        } else {
            throw MySQLExceptions.notSupportBindJavaType(nullable.getClass());
        }
        return type;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/year.html">The YEAR Type</a>
     */
    public static int bindNonNullToYear(final int batchIndex, SQLType sqlType, Value paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull;
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }

        if (value > 2155 || (value < 1901 && value != 0)) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, sqlType, paramValue);
        }
        return value;
    }

    public static String bindNonNullToSetType(final int batchIndex, SQLType sqlType, Value paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        if (!(nonNull instanceof Set)) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
        }
        final Set<?> set = (Set<?>) nonNull;
        final StringBuilder builder = new StringBuilder(set.size() * 4);
        int index = 0;
        for (Object element : set) {
            if (index > 0) {
                builder.append(',');
            }
            if (element instanceof String) {
                builder.append(element);
            } else if (element instanceof Enum) {
                builder.append(((Enum<?>) element).name());
            } else {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
            }
            index++;
        }
        return builder.toString();
    }


    public static void assertParamCountMatch(int stmtIndex, int paramCount, int bindCount)
            throws SQLException {

        if (bindCount != paramCount) {
            if (paramCount == 0) {
                throw MySQLExceptions.createNoParametersExistsError(stmtIndex);
            } else if (paramCount > bindCount) {
                throw MySQLExceptions.createParamsNotBindError(stmtIndex, bindCount);
            } else {
                throw MySQLExceptions.createInvalidParameterNoError(stmtIndex, paramCount);
            }
        }
    }

    public static void releaseOnError(Queue<ByteBuf> queue, final ByteBuf packet) {
        ByteBuf byteBuf;
        while ((byteBuf = queue.poll()) != null) {
            byteBuf.release();
        }
        queue.clear();
        if (packet.refCnt() > 0) {
            packet.release();
        }
    }

}
