package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.vendor.stmt.IBindValue;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.util.annotation.Nullable;

/**
 * @see BindStmt
 */
public final class BindValue extends JdbdParamValue implements IBindValue {

    public static BindValue wrap(int parameterIndex, PgType type, @Nullable Object value) {
        return new BindValue(parameterIndex, type, value);
    }

    public static BindValue wrap(BindValue bindValue, PgType newType) {
        return new BindValue(bindValue.getParamIndex(), newType, bindValue.getValue());
    }

    public static BindValue wrap(PgType pgType, ParamValue paramValue) {
        return new BindValue(paramValue.getParamIndex(), pgType, paramValue.getValue());
    }

    private final PgType pgType;

    private BindValue(int parameterIndex, PgType pgType, @Nullable Object value) {
        super(parameterIndex, value);
        this.pgType = pgType;
    }

    @Override
    public final PgType getType() {
        return this.pgType;
    }


}
