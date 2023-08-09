package io.jdbd.session;

import io.jdbd.Driver;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * <p>
 * This class is supported by following methods :
 * <ul>
 *     <li>{@link OptionSpec#valueOf(Option)}</li>
 *     <li>{@link io.jdbd.statement.Statement#setOption(Option, Object)}</li>
 *     <li>{@link io.jdbd.result.ResultRowMeta#getOf(int, Option)}</li>
 *     <li>{@link io.jdbd.result.ResultRowMeta#getNonNullOf(int, Option)}</li>
 * </ul>
 * for more dialectal driver.
 * </p>
 *
 * @see OptionSpec
 * @since 1.0
 */
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

    public static final Option<Boolean> AUTO_COMMIT = Option.from("AUTO COMMIT", Boolean.class);

    /**
     * @see io.jdbd.result.ServerException#valueOf(Option)
     */
    public static final Option<String> SQL_STATE = Option.from("SQL STATE", String.class);
    public static final Option<String> MESSAGE = Option.from("MESSAGE", String.class);

    /**
     * @see io.jdbd.result.ServerException#valueOf(Option)
     */
    public static final Option<Integer> VENDOR_CODE = Option.from("VENDOR CODE", Integer.class);

    /**
     * @see Option#WARNING_COUNT
     */
    public static final Option<Integer> WARNING_COUNT = Option.from("WARNING COUNT", Integer.class);

    public static final Option<String> USER = Option.from(Driver.USER, String.class);


    public static final Option<ZoneOffset> CLIENT_ZONE = Option.from("CLIENT ZONE", ZoneOffset.class);

    public static final Option<ZoneOffset> SERVER_ZONE = Option.from("SERVER ZONE", ZoneOffset.class);

    public static final Option<Charset> CLIENT_CHARSET = Option.from("CLIENT CHARSET", Charset.class);

    /**
     * true : text value support backslash escapes
     */
    public static final Option<Boolean> BACKSLASH_ESCAPES = Option.from("BACKSLASH ESCAPES", Boolean.class);

    /**
     * true : binary value support hex escapes
     */
    public static final Option<Boolean> BINARY_HEX_ESCAPES = Option.from("BINARY HEX ESCAPES", Boolean.class);

    public static final Option<Boolean> AUTO_RECONNECT = Option.from("AUTO RECONNECT", Boolean.class);


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

    private void readObject(ObjectInputStream in) throws IOException {
        throw new InvalidObjectException("can't deserialize Option");
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("can't deserialize Option");
    }


}
