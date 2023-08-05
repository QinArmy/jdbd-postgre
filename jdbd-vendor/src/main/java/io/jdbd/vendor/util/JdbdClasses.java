package io.jdbd.vendor.util;

import io.jdbd.lang.Nullable;

public abstract class JdbdClasses {

    protected JdbdClasses() {
        throw new UnsupportedOperationException();
    }


    @Nullable
    public static String safeClassName(@Nullable Object value) {
        return value == null ? null : value.getClass().getName();
    }

    public static Class<?> getEnumClass(Class<?> clazz) {
        if (!Enum.class.isAssignableFrom(clazz)) {
            String m = String.format("%s isn't enum", clazz.getName());
            throw new IllegalArgumentException(m);
        }
        if (clazz.isAnonymousClass()) {
            clazz = clazz.getSuperclass();
        }
        return clazz;
    }


}
