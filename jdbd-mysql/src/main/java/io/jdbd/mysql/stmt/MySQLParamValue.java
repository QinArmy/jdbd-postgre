package io.jdbd.mysql.stmt;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.vendor.stmt.AbstractParamValue;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.util.annotation.Nullable;

public class MySQLParamValue extends AbstractParamValue {

    public static ParamValue create(int parameterIndex, @Nullable Object value) {
        return new MySQLParamValue(parameterIndex, value);
    }


    MySQLParamValue(int parameterIndex, @Nullable Object value) {
        super(parameterIndex, value);
    }


    @Override
    protected final int getByteLengthBoundary() {
        return ClientCommandProtocol.MAX_PAYLOAD_SIZE;
    }

    @Override
    protected final int getStringLengthBoundary() {
        return ClientCommandProtocol.MAX_PAYLOAD_SIZE;
    }


}
