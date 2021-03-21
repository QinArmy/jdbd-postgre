package io.jdbd.type;

import io.jdbd.lang.Nullable;


/**
 * <p>
 * see Book Effective Java item (Use instance fields instead of ordinals).
 * </p>
 */
public interface CodeEnum {

    String name();

    int code();

    default String display() {
        return name();
    }

    @SuppressWarnings("unchecked")
    @Nullable
    static <T extends Enum<T> & CodeEnum> T resolve(Class<?> enumClass, int code) {
        throw new UnsupportedOperationException();
    }

}
