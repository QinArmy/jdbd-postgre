package io.jdbd.vendor.stmt;


public interface IBindValue extends ParamValue {

    io.jdbd.meta.SQLType getType();
}
