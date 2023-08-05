package io.jdbd.meta;

/**
 * <p>
 * This enum is a implementation of {@link DataType} for the convenience that application bind parameter.
 * </p>
 *
 * @see io.jdbd.statement.ParametrizedStatement#bind(int, DataType, Object)
 * @see io.jdbd.statement.Statement#bindStmtVar(String, DataType, Object)
 */
public enum JdbdType implements DataType {

    UNKNOWN,

    /**
     * Identifies the generic SQL value {@code NULL} . This enum instance representing sql type is unknown and value is null.
     * <p>
     *     <ul>
     *         <li>{@link io.jdbd.result.ResultRowMeta#getJdbdType(int)} return this enm instance if sql type is unknown and value is null. For example : {@code SELECT NULL as result}</li>
     *         <li>{@link io.jdbd.statement.ParametrizedStatement#bind(int, DataType, Object)} support this enum instance,if application developer don't known type and value is null.</li>
     *         <li>{@link io.jdbd.statement.Statement#bindStmtVar(String, DataType, Object)} support this enum instance,if application developer don't known type and value is null.</li>
     *     </ul>
     *     Actually in most case , application developer known the type of null ,so dont' need this enum instance. For example:
     *     <pre><br/>
     *         // stmt is io.jdbd.statement.ParametrizedStatement instance
     *         stmt.bind(0,JdbdType.BIGINT,null)
     *     </pre>
     * </p>
     */
    NULL,

    /**
     * Identifies the generic SQL type {@code BOOLEAN}.
     */
    BOOLEAN,

    /**
     * Identifies the generic SQL type {@code BIT}, not boolean.
     */
    BIT,

    /**
     * Identifies the generic SQL type {@code TINYINT}.
     */
    TINYINT,

    /**
     * Identifies the generic SQL type {@code SMALLINT}.
     */
    SMALLINT,

    MEDIUMINT,
    /**
     * Identifies the generic SQL type {@code INTEGER}.
     */
    INTEGER,
    /**
     * Identifies the generic SQL type {@code BIGINT}.
     */
    BIGINT,

    /**
     * Identifies the generic SQL type {@code TINYINT}.
     */
    TINYINT_UNSIGNED,

    /**
     * Identifies the generic SQL type {@code SMALLINT}.
     */
    SMALLINT_UNSIGNED,

    MEDIUMINT_UNSIGNED,
    /**
     * Identifies the generic SQL type {@code INTEGER}.
     */
    INTEGER_UNSIGNED,
    /**
     * Identifies the generic SQL type {@code BIGINT}.
     */
    BIGINT_UNSIGNED,

    /**
     * Identifies the generic SQL type {@code FLOAT}.
     */
    FLOAT,
    /**
     * Identifies the generic SQL type {@code REAL}.
     */
    REAL,
    /**
     * Identifies the generic SQL type {@code DOUBLE}.
     */
    DOUBLE,
    /**
     * Identifies the generic SQL type {@code NUMERIC}.
     */
    NUMERIC,
    /**
     * Identifies the generic SQL type {@code DECIMAL}.
     */
    DECIMAL,

    /**
     * Identifies the generic SQL type {@code DECIMAL}.
     */
    DECIMAL_UNSIGNED,

    /**
     * Identifies the generic SQL type {@code CHAR}.
     */
    CHAR,
    /**
     * Identifies the generic SQL type {@code VARCHAR}.
     */
    VARCHAR,

    /**
     * Identifies the generic SQL type {@code ENUM}.
     */
    ENUM,

    TINYTEXT,
    MEDIUMTEXT,
    TEXT,
    LONGTEXT,

    /**
     * Identifies the generic SQL type {@code BINARY}.
     */
    BINARY,
    /**
     * Identifies the generic SQL type {@code VARBINARY}.
     */
    VARBINARY,

    TINYBLOB,
    MEDIUMBLOB,
    BLOB,
    LONGBLOB,

    /**
     * Identifies the generic SQL type {@code TIME}.
     */
    TIME,

    YEAR,

    YEAR_MONTH,

    MONTH_DAY,

    /**
     * Identifies the generic SQL type {@code DATE}.
     */
    DATE,

    /**
     * Identifies the generic SQL type {@code TIMESTAMP}.
     */
    TIMESTAMP,

    /**
     * Identifies the generic SQL type {@code TIME_WITH_TIMEZONE}.
     */
    TIME_WITH_TIMEZONE,

    /**
     * Identifies the generic SQL type {@code TIMESTAMP_WITH_TIMEZONE}.
     */
    TIMESTAMP_WITH_TIMEZONE,

    /**
     * A time-based amount of time, such as '34.5 seconds'.
     */
    DURATION,

    /**
     * A date-based amount of time.
     */
    PERIOD,

    /**
     * A date-time-based amount of time.
     */
    INTERVAL,

    /**
     * Identifies the generic SQL type {@code ARRAY}.
     */
    ARRAY,

    /**
     * Identifies the generic SQL type {@code DATALINK}.
     */
    DATALINK,

    /**
     * Identifies the SQL type {@code ROWID}.
     */
    ROWID,

    /**
     * Identifies the generic SQL type {@code SQLXML}.
     */
    XML,

    JSON,

    JSONB,

    /**
     * Identifies the generic SQL type {@code REF}.
     */
    REF,

    /**
     * Identifies the generic SQL type {@code REF_CURSOR}.
     */
    REF_CURSOR,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    GEOMETRY,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    POINT,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    LINE_STRING,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    LINE,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    LINEAR_RING,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    POLYGON,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    GEOMETRY_COLLECTION,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    MULTI_POINT,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    MULTI_LINE_STRING,

    /**
     * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
     */
    MULTI_POLYGON,

    /**
     * Indicates that the dialect SQL type  , this enum instance is only returned by {@link io.jdbd.result.ResultRowMeta#getJdbdType(int)}.
     * This enum instance representing the dialect SQL type couldn't be expressed other instance of {@link JdbdType}.
     * <p>
     *     <ul>
     *         <li>{@link io.jdbd.statement.ParametrizedStatement#bind(int, DataType, Object)} don't support this enum instance.</li>
     *         <li>{@link io.jdbd.statement.Statement#bindStmtVar(String, DataType, Object)} don't support this enum instance.</li>
     *     </ul>
     *     If application developer want to bind dialect type ,then application developer should define the new {@link DataType} type
     *     that it's {@link #typeName()} is supported by database.
     * </p>
     */
    DIALECT_TYPE;


    @Override
    public final String typeName() {
        return this.name();
    }


    @Override
    public final boolean isArray() {
        return this == ARRAY;
    }

    @Override
    public final boolean isUnknown() {
        return this == UNKNOWN;
    }

    @Override
    public final boolean isUserDefined() {
        return false;
    }

    /**
     * @return true : unsigned number
     */
    public final boolean isUnsigned() {
        final boolean match;
        switch (this) {
            case TINYINT_UNSIGNED:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case INTEGER_UNSIGNED:
            case BIGINT_UNSIGNED:
            case DECIMAL_UNSIGNED:
                match = true;
                break;
            default:
                match = false;
        }
        return match;
    }

    /**
     * @return true :  signed number
     */
    public final boolean isSigned() {
        final boolean match;
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case NUMERIC:
            case FLOAT:
            case REAL:
            case DOUBLE:
                match = true;
                break;
            default:
                match = false;
        }
        return match;
    }


}
