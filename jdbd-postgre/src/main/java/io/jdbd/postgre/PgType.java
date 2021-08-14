package io.jdbd.postgre;


import io.jdbd.type.LongBinary;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.LongString;
import io.jdbd.type.geometry.Point;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.*;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public enum PgType implements io.jdbd.meta.SQLType {

    UNSPECIFIED(PgConstant.TYPE_UNSPECIFIED, JDBCType.NULL, Object.class),
    SMALLINT(PgConstant.TYPE_INT2, JDBCType.SMALLINT, Short.class),
    SMALLINT_ARRAY(PgConstant.TYPE_INT2_ARRAY, JDBCType.ARRAY, Short[].class),
    INTEGER(PgConstant.TYPE_INT4, JDBCType.INTEGER, Integer.class),

    INTEGER_ARRAY(PgConstant.TYPE_INT4_ARRAY, JDBCType.ARRAY, Integer[].class),
    BIGINT(PgConstant.TYPE_INT8, JDBCType.BIGINT, Long.class),
    BIGINT_ARRAY(PgConstant.TYPE_INT8_ARRAY, JDBCType.ARRAY, Long[].class),
    TEXT(PgConstant.TYPE_TEXT, JDBCType.LONGVARCHAR, LongString.class),

    TEXT_ARRAY(PgConstant.TYPE_TEXT_ARRAY, JDBCType.ARRAY, LongString[].class),
    DECIMAL(PgConstant.TYPE_NUMERIC, JDBCType.DECIMAL, BigDecimal.class),
    DECIMAL_ARRAY(PgConstant.TYPE_NUMERIC_ARRAY, JDBCType.ARRAY, BigDecimal[].class),
    FLOAT4(PgConstant.TYPE_FLOAT4, JDBCType.FLOAT, Float.class),

    FLOAT4_ARRAY(PgConstant.TYPE_FLOAT4_ARRAY, JDBCType.ARRAY, Float[].class),
    FLOAT8(PgConstant.TYPE_FLOAT8, JDBCType.DOUBLE, Double.class),
    FLOAT8_ARRAY(PgConstant.TYPE_FLOAT8_ARRAY, JDBCType.ARRAY, Double[].class),
    BOOL(PgConstant.TYPE_BOOLEAN, JDBCType.BOOLEAN, Boolean.class),

    BOOL_ARRAY(PgConstant.TYPE_BOOL_ARRAY, JDBCType.ARRAY, Boolean[].class),
    DATE(PgConstant.TYPE_DATE, JDBCType.DATE, LocalDate.class),
    DATE_ARRAY(PgConstant.TYPE_DATE_ARRAY, JDBCType.ARRAY, LocalDate[].class),
    TIME(PgConstant.TYPE_TIME, JDBCType.TIME, LocalTime.class),

    TIME_ARRAY(PgConstant.TYPE_TIME_ARRAY, JDBCType.ARRAY, LocalTime[].class),
    TIMETZ(PgConstant.TYPE_TIMETZ, JDBCType.TIME_WITH_TIMEZONE, OffsetTime.class),
    TIMETZ_ARRAY(PgConstant.TYPE_TIMETZ_ARRAY, JDBCType.ARRAY, OffsetTime[].class),
    TIMESTAMP(PgConstant.TYPE_TIMESTAMP, JDBCType.TIMESTAMP, LocalDateTime.class),

    TIMESTAMP_ARRAY(PgConstant.TYPE_TIMESTAMP_ARRAY, JDBCType.ARRAY, LocalDateTime[].class),
    TIMESTAMPTZ(PgConstant.TYPE_TIMESTAMPTZ, JDBCType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class),
    TIMESTAMPTZ_ARRAY(PgConstant.TYPE_TIMESTAMPTZ_ARRAY, JDBCType.ARRAY, OffsetDateTime[].class),
    BYTEA(PgConstant.TYPE_BYTEA, JDBCType.LONGVARBINARY, LongBinary.class),

    BYTEA_ARRAY(PgConstant.TYPE_BYTEA_ARRAY, JDBCType.ARRAY, LongBinary[].class),
    VARCHAR(PgConstant.TYPE_VARCHAR, JDBCType.VARCHAR, String.class),
    VARCHAR_ARRAY(PgConstant.TYPE_VARCHAR_ARRAY, JDBCType.ARRAY, String[].class),
    BPCHAR(PgConstant.TYPE_BPCHAR, JDBCType.NULL, Object.class),
    BPCHAR_ARRAY(PgConstant.TYPE_BPCHAR_ARRAY, JDBCType.NULL, Object.class),
    MONEY(PgConstant.TYPE_MONEY, JDBCType.VARCHAR, String.class),//java.lang.String because format dependent on locale

    MONEY_ARRAY(PgConstant.TYPE_MONEY_ARRAY, JDBCType.ARRAY, String[].class),
    BIT(PgConstant.TYPE_BIT, JDBCType.BIT, BitSet.class),
    OID(PgConstant.TYPE_OID, JDBCType.BIGINT, Long.class),

    OID_ARRAY(PgConstant.TYPE_OID_ARRAY, JDBCType.NULL, Long[].class),
    BIT_ARRAY(PgConstant.TYPE_BIT_ARRAY, JDBCType.ARRAY, BitSet[].class),
    INTERVAL(PgConstant.TYPE_INTERVAL, JDBCType.OTHER, Duration.class),
    INTERVAL_ARRAY(PgConstant.TYPE_INTERVAL_ARRAY, JDBCType.ARRAY, Duration[].class),

    CHAR(PgConstant.TYPE_CHAR, JDBCType.CHAR, String.class),
    CHAR_ARRAY(PgConstant.TYPE_CHAR_ARRAY, JDBCType.ARRAY, String[].class),
    VARBIT(PgConstant.TYPE_VARBIT, JDBCType.BIT, BitSet.class),
    VARBIT_ARRAY(PgConstant.TYPE_VARBIT_ARRAY, JDBCType.ARRAY, BitSet[].class),

    UUID(PgConstant.TYPE_UUID, JDBCType.OTHER, UUID.class),
    UUID_ARRAY(PgConstant.TYPE_UUID_ARRAY, JDBCType.ARRAY, UUID[].class),
    XML(PgConstant.TYPE_XML, JDBCType.SQLXML, String.class),
    XML_ARRAY(PgConstant.TYPE_XML_ARRAY, JDBCType.ARRAY, String[].class),

    POINT(PgConstant.TYPE_POINT, JDBCType.OTHER, Point.class),
    POINT_ARRAY(PgConstant.TYPE_POINT_ARRAY, JDBCType.ARRAY, Point[].class),
    LINE(PgConstant.TYPE_LINE, JDBCType.OTHER, String.class),
    LINE_SEGMENT(PgConstant.TYPE_LSEG, JDBCType.OTHER, String.class),

    BOX(PgConstant.TYPE_BOX, JDBCType.OTHER, String.class),
    PATH(PgConstant.TYPE_PATH, JDBCType.OTHER, LineString.class),
    POLYGON(PgConstant.TYPE_POLYGON, JDBCType.OTHER, LongString.class),
    CIRCLE(PgConstant.TYPE_CIRCLE, JDBCType.OTHER, Circle.class),

    JSONB(PgConstant.TYPE_JSONB, JDBCType.LONGVARCHAR, LongString.class),
    JSONB_ARRAY(PgConstant.TYPE_JSONB_ARRAY, JDBCType.ARRAY, LongString[].class),
    JSON(PgConstant.TYPE_JSON, JDBCType.LONGVARCHAR, LongString.class),
    JSON_ARRAY(PgConstant.TYPE_JSON_ARRAY, JDBCType.ARRAY, LongString[].class),

    REF_CURSOR(PgConstant.TYPE_REF_CURSOR, JDBCType.REF_CURSOR, Object.class),
    REF_CURSOR_ARRAY(PgConstant.TYPE_REF_CURSOR_ARRAY, JDBCType.ARRAY, Object[].class);

    private final int typeOid;

    private final JDBCType jdbcType;

    private final Class<?> javaType;

    PgType(int typeOid, JDBCType jdbcType, Class<?> javaType) {
        this.typeOid = typeOid;
        this.jdbcType = jdbcType;
        this.javaType = javaType;
    }

    @Override
    public final JDBCType jdbcType() {
        return this.jdbcType;
    }

    @Override
    public final Class<?> javaType() {
        return this.javaType;
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
        return this == FLOAT4 || this == FLOAT8;
    }

    @Override
    public final boolean isLongString() {
        return false;
    }

    @Override
    public boolean isLongBinary() {
        return false;
    }

    @Override
    public final boolean isString() {
        return false;
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isTimeType() {
        return false;
    }

    @Override
    public boolean isDecimal() {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public final String getVendor() {
        return PgType.class.getPackage().getName();
    }

    @Override
    public final Integer getVendorTypeNumber() {
        return this.typeOid;
    }

    static {
        checkTypeOid();
    }

    private static void checkTypeOid() {
        Set<Integer> set = new HashSet<>();
        for (PgType type : PgType.values()) {
            if (set.contains(type.typeOid)) {
                throw new IllegalStateException(String.format("Type[%s] oid[%s] duplication.", type, type.typeOid));
            }
            set.add(type.typeOid);
        }
    }


}
