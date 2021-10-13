package io.jdbd.postgre;


import io.jdbd.type.Interval;
import io.jdbd.type.LongBinary;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LongString;
import io.jdbd.type.geometry.Point;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.*;
import java.util.*;

public enum PgType implements io.jdbd.meta.SQLType {

    UNSPECIFIED(PgConstant.TYPE_UNSPECIFIED, JDBCType.NULL, Object.class),

    BOOLEAN(PgConstant.TYPE_BOOLEAN, JDBCType.BOOLEAN, Boolean.class),

    SMALLINT(PgConstant.TYPE_INT2, JDBCType.SMALLINT, Short.class),
    INTEGER(PgConstant.TYPE_INT4, JDBCType.INTEGER, Integer.class),
    BIGINT(PgConstant.TYPE_INT8, JDBCType.BIGINT, Long.class),
    DECIMAL(PgConstant.TYPE_NUMERIC, JDBCType.DECIMAL, BigDecimal.class),
    REAL(PgConstant.TYPE_FLOAT4, JDBCType.FLOAT, Float.class),
    DOUBLE(PgConstant.TYPE_FLOAT8, JDBCType.DOUBLE, Double.class),

    BIT(PgConstant.TYPE_BIT, JDBCType.BIT, BitSet.class),
    VARBIT(PgConstant.TYPE_VARBIT, JDBCType.BIT, BitSet.class),
    TIMESTAMP(PgConstant.TYPE_TIMESTAMP, JDBCType.TIMESTAMP, LocalDateTime.class),
    DATE(PgConstant.TYPE_DATE, JDBCType.DATE, LocalDate.class),
    TIME(PgConstant.TYPE_TIME, JDBCType.TIME, LocalTime.class),
    TIMESTAMPTZ(PgConstant.TYPE_TIMESTAMPTZ, JDBCType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class),
    TIMETZ(PgConstant.TYPE_TIMETZ, JDBCType.TIME_WITH_TIMEZONE, OffsetTime.class),
    BYTEA(PgConstant.TYPE_BYTEA, JDBCType.LONGVARBINARY, LongBinary.class),
    CHAR(PgConstant.TYPE_CHAR, JDBCType.CHAR, String.class),
    VARCHAR(PgConstant.TYPE_VARCHAR, JDBCType.VARCHAR, String.class),
    MONEY(PgConstant.TYPE_MONEY, JDBCType.VARCHAR, String.class),//java.lang.String because format dependent on locale
    TEXT(PgConstant.TYPE_TEXT, JDBCType.LONGVARCHAR, LongString.class),
    TSVECTOR(PgConstant.TYPE_TSVECTOR, JDBCType.LONGVARCHAR, LongString.class),
    TSQUERY(PgConstant.TYPE_TSQUERY, JDBCType.LONGVARCHAR, LongString.class),
    OID(PgConstant.TYPE_OID, JDBCType.BIGINT, Long.class),
    INTERVAL(PgConstant.TYPE_INTERVAL, JDBCType.OTHER, Interval.class),
    UUID(PgConstant.TYPE_UUID, JDBCType.CHAR, UUID.class),
    XML(PgConstant.TYPE_XML, JDBCType.SQLXML, LongString.class),
    POINT(PgConstant.TYPE_POINT, JDBCType.OTHER, Point.class),
    CIRCLES(PgConstant.TYPE_CIRCLE, JDBCType.OTHER, Circle.class),
    LINE_SEGMENT(PgConstant.TYPE_LSEG, JDBCType.OTHER, Line.class),
    PATH(PgConstant.TYPE_PATH, JDBCType.OTHER, LineString.class),

    // below Geometries use ResultRow.get(int,Class)
    BOX(PgConstant.TYPE_BOX, JDBCType.OTHER, String.class),
    LINE(PgConstant.TYPE_LINE, JDBCType.OTHER, String.class),
    POLYGON(PgConstant.TYPE_POLYGON, JDBCType.OTHER, String.class),

    JSON(PgConstant.TYPE_JSON, JDBCType.LONGVARCHAR, LongString.class),
    JSONB(PgConstant.TYPE_JSONB, JDBCType.LONGVARCHAR, LongString.class),
    MACADDR(PgConstant.TYPE_MAC_ADDR, JDBCType.VARCHAR, String.class),
    MACADDR8(PgConstant.TYPE_MAC_ADDR8, JDBCType.VARCHAR, String.class),
    INET(PgConstant.TYPE_INET, JDBCType.VARCHAR, String.class),
    CIDR(PgConstant.TYPE_CIDR, JDBCType.VARCHAR, String.class),

    INT4RANGE(PgConstant.TYPE_INT4RANGE, JDBCType.VARCHAR, String.class),
    INT8RANGE(PgConstant.TYPE_INT8RANGE, JDBCType.VARCHAR, String.class),
    NUMRANGE(PgConstant.TYPE_NUMRANGE, JDBCType.VARCHAR, String.class),
    TSRANGE(PgConstant.TYPE_TSRANGE, JDBCType.VARCHAR, String.class),
    TSTZRANGE(PgConstant.TYPE_TSTZRANGE, JDBCType.VARCHAR, String.class),
    DATERANGE(PgConstant.TYPE_DATERANGE, JDBCType.VARCHAR, String.class),

    REF_CURSOR(PgConstant.TYPE_REF_CURSOR, JDBCType.REF_CURSOR, Object.class),


    BOOLEAN_ARRAY(PgConstant.TYPE_BOOLEAN_ARRAY, BOOLEAN),


    SMALLINT_ARRAY(PgConstant.TYPE_INT2_ARRAY, SMALLINT),
    INTEGER_ARRAY(PgConstant.TYPE_INT4_ARRAY, INTEGER),
    BIGINT_ARRAY(PgConstant.TYPE_INT8_ARRAY, BIGINT),
    DECIMAL_ARRAY(PgConstant.TYPE_NUMERIC_ARRAY, DECIMAL),

    OID_ARRAY(PgConstant.TYPE_OID_ARRAY, OID),
    REAL_ARRAY(PgConstant.TYPE_FLOAT4_ARRAY, REAL),
    DOUBLE_ARRAY(PgConstant.TYPE_FLOAT8_ARRAY, DOUBLE),
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

    TSVECTOR_ARRAY(PgConstant.TYPE_TSVECTOR_ARRAY, TSVECTOR),
    TSQUERY_ARRAY(PgConstant.TYPE_TSQUERY_ARRAY, TSQUERY),


    POINT_ARRAY(PgConstant.TYPE_POINT_ARRAY, POINT),
    LINE_ARRAY(PgConstant.TYPE_LINE_ARRAY, LINE),
    LINE_SEGMENT_ARRAY(PgConstant.TYPE_LINE_LSEG_ARRAY, LINE_SEGMENT),
    BOX_ARRAY(PgConstant.TYPE_BOX_ARRAY, BOX),

    PATH_ARRAY(PgConstant.TYPE_PATH_ARRAY, PATH),
    POLYGON_ARRAY(PgConstant.TYPE_POLYGON_ARRAY, POLYGON),
    CIRCLES_ARRAY(PgConstant.TYPE_CIRCLES_ARRAY, CIRCLES),


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


    REF_CURSOR_ARRAY(PgConstant.TYPE_REF_CURSOR_ARRAY, REF_CURSOR);

    private static final Map<Short, PgType> CODE_TO_TYPE_MAP = createCodeToTypeMap();

    private final short typeOid;

    private final JDBCType jdbcType;

    private final Class<?> javaType;

    private final PgType elementType;

    PgType(short typeOid, JDBCType jdbcType, Class<?> javaType) {
        if (jdbcType == JDBCType.ARRAY) {
            throw new IllegalArgumentException(String.format("jdbcType[%s] error", jdbcType));
        }
        this.typeOid = typeOid;
        this.jdbcType = jdbcType;
        this.javaType = javaType;
        this.elementType = null;
    }

    PgType(short typeOid, PgType elementType) {
        this.typeOid = typeOid;
        this.jdbcType = JDBCType.ARRAY;
        this.javaType = Object.class;
        this.elementType = elementType;
    }

    @Override
    public final JDBCType jdbcType() {
        return this.jdbcType;
    }

    @Override
    public final Class<?> javaType() {
        return this.javaType;
    }

    @Nullable
    @Override
    public final PgType elementType() {
        return this.elementType;
    }


    @Override
    public final boolean isUnsigned() {
        return false;
    }

    @Override
    public final boolean isNumber() {
        return isIntegerType()
                || isFloatType()
                || isDecimal();
    }

    @Override
    public final boolean isIntegerType() {
        return this == SMALLINT
                || this == INTEGER
                || this == BIGINT;
    }


    @Override
    public final boolean isFloatType() {
        return this == REAL || this == DOUBLE;
    }

    @Override
    public final boolean isLongString() {
        return false;
    }

    @Override
    public final boolean isLongBinary() {
        return this == BYTEA || this == BYTEA_ARRAY;
    }


    @Override
    public final boolean isStringType() {
        return false;
    }

    @Override
    public boolean isBinaryType() {
        return false;
    }

    @Override
    public boolean isTimeType() {
        return false;
    }

    @Override
    public final boolean isDecimal() {
        return this == DECIMAL;
    }

    @Override
    public final boolean isCaseSensitive() {
        final boolean sensitive;
        switch (this) {
            case OID:
            case SMALLINT:
            case SMALLINT_ARRAY:
            case INTEGER:
            case INTEGER_ARRAY:
            case BIGINT:
            case BIGINT_ARRAY:
            case REAL:
            case REAL_ARRAY:
            case DOUBLE:
            case DOUBLE_ARRAY:
            case DECIMAL:
            case DECIMAL_ARRAY:
            case BOOLEAN:
            case BOOLEAN_ARRAY:
            case BIT:
            case BIT_ARRAY:
            case VARBIT:
            case VARBIT_ARRAY:
            case TIMESTAMP:
            case TIMESTAMP_ARRAY:
            case TIME:
            case TIME_ARRAY:
            case DATE:
            case DATE_ARRAY:
            case TIMESTAMPTZ:
            case TIMESTAMPTZ_ARRAY:
            case TIMETZ:
            case TIMETZ_ARRAY:
            case INTERVAL:
            case INTERVAL_ARRAY:
            case POINT:
            case POINT_ARRAY:
            case BOX:
            case BOX_ARRAY:
            case LINE:
            case LINE_ARRAY:
            case LINE_SEGMENT:
            case LINE_SEGMENT_ARRAY:
            case PATH:
            case PATH_ARRAY:
            case POLYGON:
            case POLYGON_ARRAY:
            case CIRCLES:
            case CIRCLES_ARRAY:
            case UUID:
            case UUID_ARRAY:
            case CIDR:
            case CIDR_ARRAY:
            case INET:
            case INET_ARRAY:
            case INT4RANGE:
            case INT4RANGE_ARRAY:
            case INT8RANGE:
            case INT8RANGE_ARRAY:
            case DATERANGE:
            case DATERANGE_ARRAY:
            case NUMRANGE:
            case NUMRANGE_ARRAY:
            case MACADDR:
            case MACADDR_ARRAY:
            case MACADDR8:
            case MACADDR8_ARRAY:
            case TSRANGE:
            case TSRANGE_ARRAY:
            case TSTZRANGE:
            case TSTZRANGE_ARRAY:
                sensitive = false;
                break;
            default:
                sensitive = true;
        }
        return sensitive;
    }

    @Override
    public final boolean isArray() {
        return this.jdbcType == JDBCType.ARRAY;
    }

    @Override
    public final boolean supportPublisher() {
        return supportBinaryPublisher() || supportTextPublisher();

    }

    @Override
    public boolean supportTextPublisher() {
        return isArray()
                || this == TEXT
                || this == VARCHAR
                || this == TSVECTOR
                || this == TSQUERY
                || this == XML
                || this == PATH
                || this == POLYGON
                || this == JSON
                || this == JSONB;
    }

    @Override
    public final boolean supportBinaryPublisher() {
        return this == BYTEA;
    }


    @Override
    public final String getName() {
        final String name;
        if (this.jdbcType == JDBCType.ARRAY) {
            name = "[L" + toActualTypeName();
        } else {
            name = getNonArrayTypeName();
        }
        return name;
    }


    @Override
    public final String getVendor() {
        return PgType.class.getPackage().getName();
    }

    @Override
    public final Integer getVendorTypeNumber() {
        return (int) this.typeOid;
    }

    public final int getTypeOid() {
        return this.typeOid;
    }


    /**
     * @see #getName()
     */
    private String getNonArrayTypeName() {
        final String name;
        switch (this) {
            case TIMESTAMPTZ:
                name = "TIMESTAMP WITH TIME ZONE";
                break;
            case TIMETZ:
                name = "TIME WITH TIME ZONE";
                break;
            default:
                name = toActualTypeName();
        }
        return name;
    }

    /**
     * @see #getNonArrayTypeName()
     */
    private String toActualTypeName() {
        final String name = this.name();
        final char[] array = name.toCharArray();
        boolean replace = false;
        for (int i = 0; i < array.length; i++) {
            if (array[i] == '_') {
                array[i] = ' ';
                replace = true;
            }
        }
        return replace ? new String(array) : name;
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
     */
    private static Map<Short, PgType> createCodeToTypeMap() {
        final PgType[] values = PgType.values();
        Map<Short, PgType> map = new HashMap<>((int) (values.length / 0.75f));
        for (PgType type : PgType.values()) {
            if (map.containsKey(type.typeOid)) {
                throw new IllegalStateException(String.format("Type[%s] oid[%s] duplication.", type, type.typeOid));
            }
            map.put(type.typeOid, type);
        }
        return Collections.unmodifiableMap(map);
    }


}
