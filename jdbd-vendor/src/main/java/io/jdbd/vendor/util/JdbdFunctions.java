package io.jdbd.vendor.util;

import java.util.function.Consumer;

public abstract class JdbdFunctions {

    protected JdbdFunctions() {
        throw new UnsupportedOperationException();
    }


    public static <T> Consumer<T> noActionConsumer() {
        return JdbdFunctions::noAction;
    }

    private static <T> void noAction(T value) {
        //no-op
    }


}
