package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.vendor.stmt.JdbdParamValue;
import reactor.util.annotation.Nullable;

/**
 * @see BindStmt
 */
public final class BindValue extends JdbdParamValue {

    public static BindValue create(int parameterIndex, PgType type, @Nullable Object value) {
        return new BindValue(parameterIndex, type, value);
    }

    public static BindValue create(BindValue bindValue, PgType newType) {
        return new BindValue(bindValue.getParamIndex(), newType, bindValue.getValue());
    }

    private final PgType pgType;

    private BindValue(int parameterIndex, PgType pgType, @Nullable Object value) {
        super(parameterIndex, value);
        this.pgType = pgType;
    }

    public final PgType getType() {
        return this.pgType;
    }


}
