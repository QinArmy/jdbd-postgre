package io.jdbd.vendor.statement;


public interface BindValue extends ParamValue {


    /**
     * @return one of blow:
     * <ul>
     *     <li>{@link io.jdbd.meta.SQLType}</li>
     *      <li>{@link java.sql.JDBCType}</li>
     * </ul>
     */
    java.sql.SQLType getType();



}
