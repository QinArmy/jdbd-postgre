package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;

@Deprecated
final class PgUserType implements DataType {

    JdbdType jdbdType;

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
