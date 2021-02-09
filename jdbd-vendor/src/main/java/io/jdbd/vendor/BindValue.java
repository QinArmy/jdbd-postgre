package io.jdbd.vendor;


import io.jdbd.lang.Nullable;


public interface BindValue {

    int getIndex();

    boolean isLongData();

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
