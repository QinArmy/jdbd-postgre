package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.JdbdParamValue;
import reactor.util.annotation.Nullable;

public class MySQLParamValue extends JdbdParamValue {

    public static MySQLParamValue wrap(int parameterIndex, @Nullable Object value) {
        return new MySQLParamValue(parameterIndex, value);
    }


    MySQLParamValue(int parameterIndex, @Nullable Object value) {
        super(parameterIndex, value);
    }



}
