package io.jdbd.mysql.protocol.client;


public interface BindValue extends io.jdbd.vendor.BindValue {

    @Override
    MySQLType getType();
}
