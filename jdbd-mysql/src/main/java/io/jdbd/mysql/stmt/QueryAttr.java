package io.jdbd.mysql.stmt;

import io.jdbd.mysql.MySQLType;
import reactor.util.annotation.Nullable;

import java.util.Objects;

public final class QueryAttr {

    public static QueryAttr wrap(MySQLType type, @Nullable Object value) {
        return new QueryAttr(Objects.requireNonNull(type, "type"), value);
    }

    private final MySQLType type;

    private final Object value;


    private QueryAttr(MySQLType type, @Nullable Object value) {
        this.type = type;
        this.value = value;
    }

    public MySQLType getType() {
        return this.type;
    }

    @Nullable
    public Object get() {
        return this.value;
    }


}
