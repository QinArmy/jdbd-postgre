package io.jdbd.mysql;


public interface BindValue extends io.jdbd.vendor.statement.BindValue {

    @Override
    MySQLType getType();
}
