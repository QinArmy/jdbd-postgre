package io.jdbd.meta;

import java.sql.JDBCType;

public enum JdbdType implements DataType {

    UNKNOWN,
    NULL;


    @Override
    public final JDBCType jdbcType() {
        final JDBCType type;
        switch (this) {
            case NULL:
                type = JDBCType.NULL;
                break;
            case UNKNOWN:
                type = JDBCType.OTHER;
                break;
            default:
                throw new IllegalStateException();
        }
        return type;
    }

    @Override
    public final String typeName() {
        return this.name();
    }


    @Override
    public final boolean isArray() {
        return false;
    }

    @Override
    public final boolean isUnknown() {
        return false;
    }

    @Override
    public final BooleanMode isUserDefined() {
        return BooleanMode.UNKNOWN;
    }


}
