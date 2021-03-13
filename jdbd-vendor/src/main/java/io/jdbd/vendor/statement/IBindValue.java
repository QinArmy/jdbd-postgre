package io.jdbd.vendor.statement;


import io.jdbd.lang.Nullable;


public interface IBindValue {

    int getParamIndex();

    boolean isStream();

    /**
     * @return one of blow:
     * <ul>
     *     <li>{@link io.jdbd.meta.SQLType}</li>
     *      <li>{@link java.sql.JDBCType}</li>
     * </ul>
     */
    java.sql.SQLType getType();

    @Nullable
    Object getValue();

    Object getRequiredValue() throws NullPointerException;

}
