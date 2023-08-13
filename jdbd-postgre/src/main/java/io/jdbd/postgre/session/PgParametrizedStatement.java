package io.jdbd.postgre.session;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.statement.ParametrizedStatement;

import java.util.Locale;
import java.util.Map;
import java.util.Set;


/**
 * <p>
 * This class is base class of following :
 *  <ul>
 *      <li>{@link PgPreparedStatement}</li>
 *      <li>{@link PgBindStatement}</li>
 *      <li>{@link PgMultiStatement}</li>
 *  </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class PgParametrizedStatement<S extends ParametrizedStatement> extends PgStatement<S> {

    private static final Map<String, PgType> BUILD_IN_TYPE_MAP = PgBinds.createPgTypeMap();


    Set<String> unknownTypeSet;

     PgParametrizedStatement(PgDatabaseSession<?> session) {
         super(session);
     }


    @Nullable
    final DataType mapDataType(final DataType paramType) {

        final DataType type;
        if (paramType instanceof PgType) {
            switch ((PgType) paramType) {
                case REF_CURSOR:
                case REF_CURSOR_ARRAY:
                case UNSPECIFIED:
                    type = null;
                    break;
                default:
                    type = paramType;
            }
        } else if (!(paramType instanceof JdbdType)) {
            final String upperCaseName;
            upperCaseName = paramType.typeName().toUpperCase(Locale.ROOT);
            DataType temp;
            temp = BUILD_IN_TYPE_MAP.get(upperCaseName);
            if (temp != null) {
                type = temp;
            } else if ((temp = this.session.internalOrUserTypeFunc.apply(upperCaseName)) == null) {
                type = paramType; // here , bind unknown DataType
                Set<String> unknownTypeSet = this.unknownTypeSet;
                if (unknownTypeSet == null) {
                    this.unknownTypeSet = unknownTypeSet = PgCollections.hashSet();
                }
                unknownTypeSet.add(upperCaseName);
            } else {
                type = temp;
            }
        } else switch ((JdbdType) paramType) {
            case NULL:
                type = PgType.UNSPECIFIED;
                break;
            case BOOLEAN:
                type = PgType.BOOLEAN;
                break;
            case TINYINT:
            case SMALLINT:
                type = PgType.SMALLINT;
                break;
            case MEDIUMINT:
            case INTEGER:
                type = PgType.INTEGER;
                break;
            case BIGINT:
                type = PgType.BIGINT;
                break;

            case DECIMAL:
            case NUMERIC:
                type = PgType.DECIMAL;
                break;
            case FLOAT:
            case REAL:
                type = PgType.REAL;
                break;
            case DOUBLE:
                type = PgType.FLOAT8;
                break;

            case CHAR:
                type = PgType.CHAR;
                break;
            case VARCHAR:
                type = PgType.VARCHAR;
                break;

            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                type = PgType.TEXT;
                break;

            case JSON:
                type = PgType.JSON;
                break;
            case JSONB:
                type = PgType.JSONB;
                break;
            case XML:
                type = PgType.XML;
                break;

            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                type = PgType.BYTEA;
                break;

            case TIME:
                type = PgType.TIME;
                break;
            case TIME_WITH_TIMEZONE:
                type = PgType.TIMETZ;
                break;

            case YEAR:
            case MONTH_DAY:
            case YEAR_MONTH:
            case DATE:
                type = PgType.DATE;
                break;

            case TIMESTAMP:
                type = PgType.TIMESTAMP;
                break;
            case TIMESTAMP_WITH_TIMEZONE:
                type = PgType.TIMESTAMPTZ;
                break;

            case BIT:
                type = PgType.BIT;
                break;
            case VARBIT:
                type = PgType.VARBIT;
                break;

            case DURATION:
            case PERIOD:
            case INTERVAL:
                type = PgType.INTERVAL;
                break;

            case TINYINT_UNSIGNED:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case INTEGER_UNSIGNED:
            case BIGINT_UNSIGNED:
            case DECIMAL_UNSIGNED:

            case UNKNOWN:
            case ARRAY:
            case INTERNAL_USE:
            case COMPOSITE:
            case USER_DEFINED:
            case ROWID:
            case REF_CURSOR:
            case GEOMETRY:
            case DIALECT_TYPE:
            default:
                type = null;
        }
        return type;
    }


}
