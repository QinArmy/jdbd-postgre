package io.jdbd.mysql.protocol.client;


import io.jdbd.vendor.IBindValue;

public interface BindValue extends IBindValue {

    @Override
    MySQLType getType();
}
