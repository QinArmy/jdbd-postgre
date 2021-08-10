package io.jdbd.meta;

import io.jdbd.result.ResultRow;

import java.sql.JDBCType;

/**
 * @since 1.0
 */
public interface SQLType extends java.sql.SQLType {

    /**
     * Returns the {@code SQLType} upper case name that represents a SQL data type.
     *
     * @return The upper case name of this {@code SQLType}.
     * @see #getName()
     */
    String name();

    JDBCType jdbcType();

    /**
     * @see ResultRow#get(String)
     */
    Class<?> javaType();


    /**
     * @return tue : type is number and unsigned.
     */
    boolean isUnsigned();

    boolean isNumber();

    boolean isIntegerType();

    boolean isFloatType();

    boolean isLongString();

    boolean isLongBinary();

    boolean isString();

    boolean isBinary();

    boolean isTimeType();

    boolean isDecimal();

}
