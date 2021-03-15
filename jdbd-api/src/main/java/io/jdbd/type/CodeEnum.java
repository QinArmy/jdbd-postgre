package io.jdbd.type;

import io.jdbd.lang.Nullable;

public interface CodeEnum {

    String name();

    int code();

    default String display() {
        return name();
    }

    @SuppressWarnings("unchecked")
    @Nullable
    static <T extends Enum<T> & CodeEnum> CodeEnum resolve(Class<?> enumClass, int code) {
        throw new UnsupportedOperationException();
    }

}
