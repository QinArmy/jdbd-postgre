package io.jdbd.meta;

import java.sql.JDBCType;

/**
 * @since 1.0
 */
public interface SQLType extends java.sql.SQLType {

    /**
     * Returns the {@code SQLType} upper case name that represents a SQL data type.
     *
     * @return The upper case name of this {@code SQLType}.
     */
    String name();

    JDBCType jdbcType();

    /**
     * @see io.jdbd.ResultRow#get(String)
     */
    Class<?> javaType();


    boolean isUnsigned();

}
