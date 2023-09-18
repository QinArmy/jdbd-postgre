package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.UserDefinedType;

final class PgUserDefinedType implements UserDefinedType {

    int oid;

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

}
