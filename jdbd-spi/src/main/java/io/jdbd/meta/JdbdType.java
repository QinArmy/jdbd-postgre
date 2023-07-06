package io.jdbd.meta;

import java.sql.JDBCType;


/**
 * @see JDBCType
 */
public enum JdbdType implements DataType {

    UNKNOWN,

    /**
     * Identifies the generic SQL value {@code NULL}.
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
     * Identifies the generic SQL type {@code CHAR}.
     */
    CHAR,
    /**
     * Identifies the generic SQL type {@code VARCHAR}.
     */
    VARCHAR,

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
     * Identifies the generic SQL type {@code DATE}.
     */
    DATE,
    /**
     * Identifies the generic SQL type {@code TIME}.
     */
    TIME,
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
     * Indicates that the SQL type
     * is database-specific and gets mapped to a Java object that can be
     * accessed via the methods getObject and setObject.
     */
    JAVA_OBJECT,
    /**
     * Identifies the generic SQL type {@code DISTINCT}.
     */
    DISTINCT,
    /**
     * Identifies the generic SQL type {@code STRUCT}.
     */
    STRUCT,
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
    SQLXML,

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
     * Indicates that the SQL type
     * is database-specific and gets mapped to a Java object that can be
     * accessed via the methods getObject and setObject.
     */
    OTHER;


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
        return false;
    }

    @Override
    public final BooleanMode isUserDefined() {
        return BooleanMode.UNKNOWN;
    }


}
