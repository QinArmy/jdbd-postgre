package io.jdbd.mysql;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.vendor.statement.AbstractParamValue;


public final class MySQLBindValue extends AbstractParamValue implements BindValue {

    public static MySQLBindValue create(int parameterIndex, MySQLType type, @Nullable Object value) {
        if (parameterIndex < 0) {
            throw new IllegalArgumentException(String.format("parameterIndex[%s]", parameterIndex));
        }
        return new MySQLBindValue(parameterIndex, type, value);
    }

    public static MySQLBindValue create(BindValue bindValue, MySQLType newType) {
        return new MySQLBindValue(bindValue.getParamIndex(), newType, bindValue.getValue());
    }


    private final MySQLType type;


    private MySQLBindValue(int parameterIndex, MySQLType type, @Nullable Object value) {
        super(parameterIndex, type == MySQLType.NULL ? null : value);
        this.type = type;
    }

    @Override
    public final MySQLType getType() {
        return this.type;
    }


    @Override
    protected int getByteLengthBoundary() {
        return ClientCommandProtocol.MAX_PAYLOAD_SIZE;
    }

    @Override
    protected int getStringLengthBoundary() {
        return ClientCommandProtocol.MAX_PAYLOAD_SIZE;
    }



}
