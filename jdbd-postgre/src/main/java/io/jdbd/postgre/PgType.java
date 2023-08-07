package io.jdbd.postgre;


import io.jdbd.meta.JdbdType;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.type.Interval;
import io.jdbd.type.Point;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.time.*;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * <p>
 * This enum is postgre build-in data type.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype.html">Data Types</a>
 */
public enum PgType implements SQLType {

    UNSPECIFIED(PgConstant.TYPE_UNSPECIFIED, JdbdType.UNKNOWN, Object.class),

    BOOLEAN(PgConstant.TYPE_BOOLEAN, JdbdType.BOOLEAN, Boolean.class),

    SMALLINT(PgConstant.TYPE_INT2, JdbdType.SMALLINT, Short.class),
    INTEGER(PgConstant.TYPE_INT4, JdbdType.INTEGER, Integer.class),
    BIGINT(PgConstant.TYPE_INT8, JdbdType.BIGINT, Long.class),
    REAL(PgConstant.TYPE_FLOAT4, JdbdType.FLOAT, Float.class),
    FLOAT8(PgConstant.TYPE_FLOAT8, JdbdType.DOUBLE, Double.class),

    DECIMAL(PgConstant.TYPE_NUMERIC, JdbdType.DECIMAL, BigDecimal.class),


    BIT(PgConstant.TYPE_BIT, JdbdType.BIT, BitSet.class),
    VARBIT(PgConstant.TYPE_VARBIT, JdbdType.BIT, BitSet.class),

    TIME(PgConstant.TYPE_TIME, JdbdType.TIME, LocalTime.class),
    TIMETZ(PgConstant.TYPE_TIMETZ, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.class),
    DATE(PgConstant.TYPE_DATE, JdbdType.DATE, LocalDate.class),
    TIMESTAMP(PgConstant.TYPE_TIMESTAMP, JdbdType.TIMESTAMP, LocalDateTime.class),
    TIMESTAMPTZ(PgConstant.TYPE_TIMESTAMPTZ, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class),

    BYTEA(PgConstant.TYPE_BYTEA, JdbdType.VARBINARY, byte[].class),
    CHAR(PgConstant.TYPE_CHAR, JdbdType.CHAR, String.class),
    VARCHAR(PgConstant.TYPE_VARCHAR, JdbdType.VARCHAR, String.class),
    MONEY(PgConstant.TYPE_MONEY, JdbdType.DIALECT_TYPE, String.class),//java.lang.String because format dependent on locale
    TEXT(PgConstant.TYPE_TEXT, JdbdType.TEXT, String.class),
    TSVECTOR(PgConstant.TYPE_TSVECTOR, JdbdType.DIALECT_TYPE, String.class),
    TSQUERY(PgConstant.TYPE_TSQUERY, JdbdType.DIALECT_TYPE, String.class),
    OID(PgConstant.TYPE_OID, JdbdType.BIGINT, Long.class),
    INTERVAL(PgConstant.TYPE_INTERVAL, JdbdType.INTERVAL, Interval.class),
    UUID(PgConstant.TYPE_UUID, JdbdType.DIALECT_TYPE, UUID.class),
    XML(PgConstant.TYPE_XML, JdbdType.XML, String.class),

    POINT(PgConstant.TYPE_POINT, JdbdType.GEOMETRY, Point.class),
    CIRCLE(PgConstant.TYPE_CIRCLE, JdbdType.DIALECT_TYPE, String.class),
    LSEG(PgConstant.TYPE_LSEG, JdbdType.DIALECT_TYPE, String.class),
    PATH(PgConstant.TYPE_PATH, JdbdType.DIALECT_TYPE, String.class),

    // below Geometries use ResultRow.get(int,Class)
    BOX(PgConstant.TYPE_BOX, JdbdType.DIALECT_TYPE, String.class),
    LINE(PgConstant.TYPE_LINE, JdbdType.DIALECT_TYPE, String.class),
    POLYGON(PgConstant.TYPE_POLYGON, JdbdType.DIALECT_TYPE, String.class),

    JSON(PgConstant.TYPE_JSON, JdbdType.JSON, String.class),
    JSONB(PgConstant.TYPE_JSONB, JdbdType.JSONB, String.class),

    JSONPATH(PgConstant.TYPE_JSONPATH, JdbdType.DIALECT_TYPE, String.class),

    MACADDR(PgConstant.TYPE_MAC_ADDR, JdbdType.DIALECT_TYPE, String.class),
    MACADDR8(PgConstant.TYPE_MAC_ADDR8, JdbdType.DIALECT_TYPE, String.class),
    INET(PgConstant.TYPE_INET, JdbdType.DIALECT_TYPE, String.class),
    CIDR(PgConstant.TYPE_CIDR, JdbdType.DIALECT_TYPE, String.class),

    INT4RANGE(PgConstant.TYPE_INT4RANGE, JdbdType.DIALECT_TYPE, String.class),
    INT8RANGE(PgConstant.TYPE_INT8RANGE, JdbdType.DIALECT_TYPE, String.class),
    NUMRANGE(PgConstant.TYPE_NUMRANGE, JdbdType.DIALECT_TYPE, String.class),

    DATERANGE(PgConstant.TYPE_DATERANGE, JdbdType.DIALECT_TYPE, String.class),
    TSRANGE(PgConstant.TYPE_TSRANGE, JdbdType.DIALECT_TYPE, String.class),
    TSTZRANGE(PgConstant.TYPE_TSTZRANGE, JdbdType.DIALECT_TYPE, String.class),


    INT4MULTIRANGE(PgConstant.TYPE_INT4MULTIRANGE, JdbdType.DIALECT_TYPE, String.class),
    INT8MULTIRANGE(PgConstant.TYPE_INT8MULTIRANGE, JdbdType.DIALECT_TYPE, String.class),
    NUMMULTIRANGE(PgConstant.TYPE_NUMMULTIRANGE, JdbdType.DIALECT_TYPE, String.class),

    DATEMULTIRANGE(PgConstant.TYPE_DATEMULTIRANGE, JdbdType.DIALECT_TYPE, String.class),
    TSMULTIRANGE(PgConstant.TYPE_TSMULTIRANGE, JdbdType.DIALECT_TYPE, String.class),
    TSTZMULTIRANGE(PgConstant.TYPE_TSTZMULTIRANGE, JdbdType.DIALECT_TYPE, String.class),


    REF_CURSOR(PgConstant.TYPE_REF_CURSOR, JdbdType.REF_CURSOR, Object.class),//TODO fix java type


    BOOLEAN_ARRAY(PgConstant.TYPE_BOOLEAN_ARRAY, BOOLEAN),

    SMALLINT_ARRAY(PgConstant.TYPE_INT2_ARRAY, SMALLINT),
    INTEGER_ARRAY(PgConstant.TYPE_INT4_ARRAY, INTEGER),
    BIGINT_ARRAY(PgConstant.TYPE_INT8_ARRAY, BIGINT),
    DECIMAL_ARRAY(PgConstant.TYPE_NUMERIC_ARRAY, DECIMAL),

    OID_ARRAY(PgConstant.TYPE_OID_ARRAY, OID),
    REAL_ARRAY(PgConstant.TYPE_FLOAT4_ARRAY, REAL),
    DOUBLE_ARRAY(PgConstant.TYPE_FLOAT8_ARRAY, FLOAT8),
    MONEY_ARRAY(PgConstant.TYPE_MONEY_ARRAY, MONEY),

    TIME_ARRAY(PgConstant.TYPE_TIME_ARRAY, TIME),
    DATE_ARRAY(PgConstant.TYPE_DATE_ARRAY, DATE),
    TIMESTAMP_ARRAY(PgConstant.TYPE_TIMESTAMP_ARRAY, TIMESTAMP),
    TIMETZ_ARRAY(PgConstant.TYPE_TIMETZ_ARRAY, TIMETZ),

    TIMESTAMPTZ_ARRAY(PgConstant.TYPE_TIMESTAMPTZ_ARRAY, TIMESTAMPTZ),
    INTERVAL_ARRAY(PgConstant.TYPE_INTERVAL_ARRAY, INTERVAL),


    BYTEA_ARRAY(PgConstant.TYPE_BYTEA_ARRAY, BYTEA),


    CHAR_ARRAY(PgConstant.TYPE_CHAR_ARRAY, CHAR),
    VARCHAR_ARRAY(PgConstant.TYPE_VARCHAR_ARRAY, VARCHAR),
    TEXT_ARRAY(PgConstant.TYPE_TEXT_ARRAY, TEXT),
    BIT_ARRAY(PgConstant.TYPE_BIT_ARRAY, BIT),

    VARBIT_ARRAY(PgConstant.TYPE_VARBIT_ARRAY, VARBIT),
    XML_ARRAY(PgConstant.TYPE_XML_ARRAY, XML),
    JSON_ARRAY(PgConstant.TYPE_JSON_ARRAY, JSON),
    JSONB_ARRAY(PgConstant.TYPE_JSONB_ARRAY, JSONB),
    JSONPATH_ARRAY(PgConstant.TYPE_JSONPATH_ARRAY, JSONPATH),

    TSVECTOR_ARRAY(PgConstant.TYPE_TSVECTOR_ARRAY, TSVECTOR),
    TSQUERY_ARRAY(PgConstant.TYPE_TSQUERY_ARRAY, TSQUERY),


    POINT_ARRAY(PgConstant.TYPE_POINT_ARRAY, POINT),
    LINE_ARRAY(PgConstant.TYPE_LINE_ARRAY, LINE),
    LSEG_ARRAY(PgConstant.TYPE_LINE_LSEG_ARRAY, LSEG),
    BOX_ARRAY(PgConstant.TYPE_BOX_ARRAY, BOX),

    PATH_ARRAY(PgConstant.TYPE_PATH_ARRAY, PATH),
    POLYGON_ARRAY(PgConstant.TYPE_POLYGON_ARRAY, POLYGON),
    CIRCLE_ARRAY(PgConstant.TYPE_CIRCLES_ARRAY, CIRCLE),


    UUID_ARRAY(PgConstant.TYPE_UUID_ARRAY, UUID),


    CIDR_ARRAY(PgConstant.TYPE_CIDR_ARRAY, CIDR),
    INET_ARRAY(PgConstant.TYPE_INET_ARRAY, INET),
    MACADDR_ARRAY(PgConstant.TYPE_MACADDR_ARRAY, MACADDR),
    MACADDR8_ARRAY(PgConstant.TYPE_MACADDR8_ARRAY, MACADDR8),

    INT4RANGE_ARRAY(PgConstant.TYPE_INT4RANGE_ARRAY, INT4RANGE),
    TSRANGE_ARRAY(PgConstant.TYPE_TSRANGE_ARRAY, TSRANGE),
    TSTZRANGE_ARRAY(PgConstant.TYPE_TSTZRANGE_ARRAY, TSTZRANGE),
    DATERANGE_ARRAY(PgConstant.TYPE_DATERANGE_ARRAY, DATERANGE),
    INT8RANGE_ARRAY(PgConstant.TYPE_INT8RANGE_ARRAY, INT8RANGE),
    NUMRANGE_ARRAY(PgConstant.TYPE_NUMRANGE_ARRAY, NUMRANGE),


    INT4MULTIRANGE_ARRAY(PgConstant.TYPE_INT4MULTIRANGE_ARRAY, INT4MULTIRANGE),
    INT8MULTIRANGE_ARRAY(PgConstant.TYPE_INT8MULTIRANGE_ARRAY, INT8MULTIRANGE),
    NUMMULTIRANGE_ARRAY(PgConstant.TYPE_NUMMULTIRANGE_ARRAY, NUMMULTIRANGE),

    DATEMULTIRANGE_ARRAY(PgConstant.TYPE_DATEMULTIRANGE_ARRAY, DATEMULTIRANGE),
    TSMULTIRANGE_ARRAY(PgConstant.TYPE_TSMULTIRANGE_ARRAY, TSMULTIRANGE),
    TSTZMULTIRANGE_ARRAY(PgConstant.TYPE_TSTZMULTIRANGE_ARRAY, TSTZMULTIRANGE),


    REF_CURSOR_ARRAY(PgConstant.TYPE_REF_CURSOR_ARRAY, REF_CURSOR);

    private static final Map<Short, PgType> CODE_TO_TYPE_MAP = createCodeToTypeMap();

    private final short typeOid;

    private final String typeName;

    private final JdbdType jdbdType;

    private final Class<?> javaType;

    private final PgType elementType;


    PgType(short typeOid, JdbdType jdbdType, Class<?> javaType) {
        if (jdbdType == JdbdType.ARRAY) {
            throw new IllegalArgumentException(String.format("jdbcType[%s] error", jdbdType));
        }
        this.typeOid = typeOid;
        this.typeName = this.name();
        this.jdbdType = jdbdType;
        this.javaType = javaType;

        this.elementType = null;
    }

    PgType(short typeOid, PgType elementType) {
        this.typeOid = typeOid;
        this.typeName = elementType.name() + "[]";
        this.jdbdType = JdbdType.ARRAY;
        this.javaType = Object.class;
        this.elementType = elementType;
    }


    @Override
    public final String typeName() {
        return this.typeName;
    }

    @Override
    public final JdbdType jdbdType() {
        return this.jdbdType;
    }

    @Override
    public final Class<?> firstJavaType() {
        return this.javaType;
    }

    @Override
    public final Class<?> secondJavaType() {
        //TODO
        return null;
    }

    @Nullable
    @Override
    public final PgType elementType() {
        return this.elementType;
    }

    @Override
    public final boolean isArray() {
        return this.jdbdType == JdbdType.ARRAY;
    }

    @Override
    public final boolean isUnknown() {
        return this == UNSPECIFIED;
    }

    @Override
    public final boolean isUserDefined() {
        return false;
    }


    @Override
    public final String vendor() {
        return PgDriver.DRIVER_VENDOR;
    }


    public static PgType from(final int oid) {
        if (oid > Short.MAX_VALUE) {
            return PgType.UNSPECIFIED;
        }
        final short typeOid = (short) oid;
        final PgType pgType;
        switch (typeOid) {
            case PgConstant.TYPE_BPCHAR:
                pgType = PgType.CHAR;
                break;
            case PgConstant.TYPE_BPCHAR_ARRAY:
                pgType = PgType.CHAR_ARRAY;
                break;
            default:
                pgType = CODE_TO_TYPE_MAP.getOrDefault(typeOid, PgType.UNSPECIFIED);
        }
        return pgType;
    }


    /**
     * @return a unmodified map.
     * @see #CODE_TO_TYPE_MAP
     */
    private static Map<Short, PgType> createCodeToTypeMap() {
        final PgType[] values = PgType.values();
        Map<Short, PgType> map = PgCollections.hashMap((int) (values.length / 0.75f));
        for (PgType type : PgType.values()) {
            if (map.containsKey(type.typeOid)) {
                throw new IllegalStateException(String.format("Type[%s] oid[%s] duplication.", type, type.typeOid));
            }
            map.put(type.typeOid, type);
        }
        return Collections.unmodifiableMap(map);
    }


//            case CHAR:
//            case VARCHAR:
//            case TEXT:
//            case TSQUERY:
//            case TSVECTOR:
//
//            case TIME:
//            case TIMETZ:
//            case DATE:
//            case TIMESTAMP:
//            case TIMESTAMPTZ:
//            case INTERVAL:
//
//            case INT4RANGE:
//            case INT8RANGE:
//            case NUMRANGE:
//            case DATERANGE:
//            case TSRANGE:
//            case TSTZRANGE:
//
//            case INT4MULTIRANGE:
//            case INT8MULTIRANGE:
//            case NUMMULTIRANGE:
//            case DATEMULTIRANGE:
//            case TSMULTIRANGE:
//            case TSTZMULTIRANGE:
//
//            case JSON:
//            case JSONB:
//            case JSONPATH:
//            case XML:
//
//            case POINT:
//            case LINE:
//            case PATH:
//            case CIRCLE:
//            case BOX:
//            case POLYGON:
//            case LSEG:
//
//            case CIDR:
//            case INET:
//            case MACADDR:
//            case MACADDR8:
//
//            case MONEY:
//            case BIT:
//            case VARBIT:
//            case UNSPECIFIED:
//            case REF_CURSOR:


//            case BOOLEAN_ARRAY:
//            case SMALLINT_ARRAY:
//            case INTEGER_ARRAY:
//            case OID_ARRAY:
//            case BIGINT_ARRAY:
//            case DECIMAL_ARRAY:
//            case REAL_ARRAY:
//            case DOUBLE_ARRAY:
//
//            case BYTEA_ARRAY:
//
//            case TIME_ARRAY:
//            case TIMETZ_ARRAY:
//            case DATE_ARRAY:
//            case TIMESTAMP_ARRAY:
//            case TIMESTAMPTZ_ARRAY:
//            case INTERVAL_ARRAY:
//
//            case MONEY_ARRAY:
//
//            case UUID_ARRAY:
//
//            case BIT_ARRAY:
//            case VARBIT_ARRAY:
//
//            case CHAR_ARRAY:
//            case VARCHAR_ARRAY:
//            case TEXT_ARRAY:
//            case JSON_ARRAY:
//            case JSONB_ARRAY:
//            case JSONPATH_ARRAY:
//            case XML_ARRAY:
//            case TSQUERY_ARRAY:
//            case TSVECTOR_ARRAY:
//
//            case INT4RANGE_ARRAY:
//            case INT8RANGE_ARRAY:
//            case NUMRANGE_ARRAY:
//            case DATERANGE_ARRAY:
//            case TSRANGE_ARRAY:
//            case TSTZRANGE_ARRAY:
//
//            case INT4MULTIRANGE_ARRAY:
//            case INT8MULTIRANGE_ARRAY:
//            case NUMMULTIRANGE_ARRAY:
//            case DATEMULTIRANGE_ARRAY:
//            case TSMULTIRANGE_ARRAY:
//            case TSTZMULTIRANGE_ARRAY:
//
//            case POINT_ARRAY:
//            case LINE_ARRAY:
//            case PATH_ARRAY:
//            case BOX_ARRAY:
//            case LSEG_ARRAY:
//            case CIRCLE_ARRAY:
//            case POLYGON_ARRAY:
//
//            case CIDR_ARRAY:
//            case INET_ARRAY:
//            case MACADDR_ARRAY:
//            case MACADDR8_ARRAY:
//
//            case REF_CURSOR_ARRAY:


}
