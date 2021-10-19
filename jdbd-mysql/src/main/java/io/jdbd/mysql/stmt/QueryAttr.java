package io.jdbd.mysql.stmt;

import io.jdbd.mysql.MySQLType;
import io.jdbd.vendor.stmt.NamedValue;
import reactor.util.annotation.Nullable;

import java.util.Objects;

public final class QueryAttr implements NamedValue {

    public static QueryAttr wrap(String name, MySQLType type, @Nullable Object value) {
        Objects.requireNonNull(type, "type");
        return new QueryAttr(name, type, value);
    }

    private final String name;

    private final MySQLType type;

    private final Object value;


    private QueryAttr(String name, MySQLType type, @Nullable Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public MySQLType getType() {
        return this.type;
    }

    @Nullable
    @Override
    public Object get() {
        return this.value;
    }

    @Override
    public Object getNonNull() {
        final Object value = this.value;
        if (value == null) {
            throw new NullPointerException("this.value is null");
        }
        return value;
    }


}
