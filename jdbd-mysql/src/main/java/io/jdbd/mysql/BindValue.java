package io.jdbd.mysql;


import io.jdbd.vendor.statement.IBindValue;

public interface BindValue extends IBindValue {

    @Override
    MySQLType getType();
}
