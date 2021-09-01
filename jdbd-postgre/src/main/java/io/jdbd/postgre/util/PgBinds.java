package io.jdbd.postgre.util;

import io.jdbd.postgre.PgType;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Point;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.Objects;

public abstract class PgBinds {

    protected PgBinds() {
        throw new UnsupportedOperationException();
    }


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
                pgType = nullable == null ? PgType.UNSPECIFIED : mapPgArrayType(nullable);
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
                if (nullable instanceof Point) {
                    pgType = PgType.POINT;
                } else if (nullable instanceof Circle) {
                    pgType = PgType.CIRCLE;
                } else if (nullable instanceof Enum) {
                    pgType = PgType.VARCHAR;
                } else {
                    pgType = PgType.UNSPECIFIED;
                }
            }
            break;
            default:
                throw PgExceptions.createUnexpectedEnumException(jdbcType);
        }
        return pgType;
    }


    private static PgType mapPgArrayType(Object nonNull) {
        throw new UnsupportedOperationException("TODO");
    }
}
