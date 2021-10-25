package io.jdbd.mysql.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.vendor.stmt.TypeValue;


public final class BindValue extends MySQLParamValue implements TypeValue {

    public static BindValue wrap(int parameterIndex, MySQLType type, @Nullable Object value) {
        if (parameterIndex < 0) {
            throw new IllegalArgumentException(String.format("parameterIndex[%s]", parameterIndex));
        }
        return new BindValue(parameterIndex, type, value);
    }

    public static BindValue wrap(BindValue bindValue, MySQLType newType) {
        return new BindValue(bindValue.getIndex(), newType, bindValue.get());
    }


    private final MySQLType type;


    private BindValue(int index, MySQLType type, @Nullable Object value) {
        super(index, value);
        this.type = type;
    }

    @Override
    public MySQLType getType() {
        return this.type;
    }


}
