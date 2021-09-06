package io.jdbd.vendor.stmt;


/**
 * <p>
 * This interface representing {@link ParamValue} can wrap {@link io.jdbd.meta.SQLType}.
 * </p>
 */
public interface IBindValue extends ParamValue {

    io.jdbd.meta.SQLType getType();
}
