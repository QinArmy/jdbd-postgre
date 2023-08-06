package io.jdbd.session;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class Option<T> {


    @SuppressWarnings("unchecked")
    public static <T> Option<T> from(final String name, final Class<T> javaType) {
        if (Isolation.hasNoText(name)) {
            throw new IllegalArgumentException("no text");
        }
        Objects.requireNonNull(javaType);
        final Option<?> option;
        option = INSTANCE_MAP.computeIfAbsent(name, k -> new Option<>(name, javaType));

        if (option.javaType == javaType) {
            return (Option<T>) option;
        }
        return new Option<>(name, javaType);
    }

    private static final ConcurrentMap<String, Option<?>> INSTANCE_MAP = Isolation.concurrentHashMap();


    public static final Option<Isolation> ISOLATION = Option.from("ISOLATION", Isolation.class);

    public static final Option<Boolean> READ_ONLY = Option.from("READ ONLY", Boolean.class);

    public static final Option<Boolean> IN_TRANSACTION = Option.from("IN TRANSACTION", Boolean.class);

    public static final Option<String> CURSOR_NAME = Option.from("CURSOR NAME", String.class);

    private final String name;

    private final Class<T> javaType;

    /**
     * private constructor
     */
    private Option(String name, Class<T> javaType) {
        this.name = name;
        this.javaType = javaType;
    }


    public String name() {
        return this.name;
    }

    public Class<T> javaType() {
        return this.javaType;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.javaType);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Option) {
            final Option<?> o = (Option<?>) obj;
            match = o.name.equals(this.name) && o.javaType == this.javaType;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return String.format("%s[ name : %s , javaType : %s , hash : %s]",
                Option.class.getName(),
                this.name,
                this.javaType.getName(),
                System.identityHashCode(this)
        );
    }



    /*-------------------below private method -------------------*/

    private void readObject(ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        throw new InvalidObjectException("can't deserialize Option");
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("can't deserialize Option");
    }


}
