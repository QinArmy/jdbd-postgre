package io.jdbd.vendor.geometry;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * {@link OutputStream#write(byte[])} and {@link java.io.Writer#write(String)} can't use {@link Consumer}
 */
@FunctionalInterface
interface IoConsumer<T> {

    void next(T t) throws IOException;

    default IoConsumer<T> andThen(final IoConsumer<? super T> after) {
        Objects.requireNonNull(after);
        return (T t) -> {
            next(t);
            after.next(t);
        };
    }

    default IoConsumer<T> after(final IoConsumer<? super T> before) {
        Objects.requireNonNull(before);
        return (T t) -> {
            before.next(t);
            next(t);
        };
    }

}
