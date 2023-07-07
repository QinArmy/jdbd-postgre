package io.jdbd.statement;

import io.jdbd.lang.Nullable;

import java.util.Objects;

/**
 * <p>
 * This class is standard implementation of {@link OutParameter}.
 * </p>
 *
 * @since 1.0
 */
final class JdbdOutParameter implements OutParameter {

    static JdbdOutParameter create(@Nullable String name, @Nullable Object value) {
        if (name == null) {
            throw new NullPointerException("out parameter name must non-null");
        }
        return new JdbdOutParameter(name, value);
    }


    private final String name;


    private final Object value;

    /**
     * private constructor
     */
    private JdbdOutParameter(String name, @Nullable Object value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Object value() {
        return this.value;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.value);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof JdbdOutParameter) {
            final JdbdOutParameter o = (JdbdOutParameter) obj;
            match = o.name.equals(this.name) && Objects.equals(o.value, this.value);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getName())
                .append("[ name : ")
                .append(this.name)
                .append(" , value : ");
        if (this.value instanceof String) {
            builder.append('?');
        } else {
            builder.append(this.value);
        }
        return builder.append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
