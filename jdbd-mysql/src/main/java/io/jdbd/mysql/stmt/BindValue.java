package io.jdbd.mysql.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.vendor.stmt.ParamValue;


public final class BindValue extends MySQLParamValue implements ParamValue {

    public static BindValue create(int parameterIndex, MySQLType type, @Nullable Object value) {
        if (parameterIndex < 0) {
            throw new IllegalArgumentException(String.format("parameterIndex[%s]", parameterIndex));
        }
        return new BindValue(parameterIndex, type, value);
    }

    public static BindValue create(BindValue bindValue, MySQLType newType) {
        return new BindValue(bindValue.getIndex(), newType, bindValue.get());
    }


    private final MySQLType type;


    private BindValue(int parameterIndex, MySQLType type, @Nullable Object value) {
        super(parameterIndex, type == MySQLType.NULL ? null : value);
        this.type = type;
    }

    public final MySQLType getType() {
        return this.type;
    }


}
