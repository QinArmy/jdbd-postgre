package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;

final class UserDefinedType implements DataType {

    @Override
    public String name() {
        return this.typeName();
    }

    @Override
    public String typeName() {
        return null;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public boolean isUnknown() {
        return false;
    }

    @Override
    public boolean isUserDefined() {
        return false;
    }
}
