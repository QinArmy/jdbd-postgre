package io.jdbd.vendor.util;

import java.util.function.Consumer;

public abstract class JdbdFunctions {

    protected JdbdFunctions() {
        throw new UnsupportedOperationException();
    }

    private static final Consumer<?> NO_ACTION_CONSUMER = c -> {
    };


    @SuppressWarnings("unchecked")
    public static <T> Consumer<T> noActionConsumer() {
        return (Consumer<T>) NO_ACTION_CONSUMER;
    }


}
