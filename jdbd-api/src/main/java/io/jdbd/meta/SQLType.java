package io.jdbd.meta;

import java.sql.JDBCType;
import java.util.List;

public interface SQLType extends java.sql.SQLType {

    /**
     * Returns the {@code SQLType} upper case name that represents a SQL data type.
     *
     * @return The upper case name of this {@code SQLType}.
     */
    String name();

    JDBCType jdbcType();

    /**
     * @see io.jdbd.ResultRow#getObject(String)
     */
    Class<?> javaType();

    /**
     * Returns the name of the vendor that supports this data type. The value
     * returned typically is the package name for this vendor.
     *
     * @return The name of the vendor for this data type
     */
    String getVendor();

    boolean isUnsigned();

    List<? extends SQLType> getFamily();

}
