package io.jdbd;

import io.jdbd.lang.Nullable;

import java.sql.JDBCType;

public interface MultiStatement extends Statement, AutoCloseable {


    MultiStatement add(String sql);

    MultiStatement bind(int index, JDBCType jdbcType, @Nullable Object nullableValue);

    MultiStatement bind(int index, io.jdbd.meta.SQLType sqlType, @Nullable Object nullableValue);


    MultiStatement bind(int index, @Nullable Object nullableValue);

    MultiResults execute();

}
