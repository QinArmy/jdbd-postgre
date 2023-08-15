package io.jdbd.vendor.env;

import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdStrings;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Consumer;

/**
 * @see Environment
 */
public abstract class Key<T> {


    public final String name;

    public final Class<T> valueClass;

    public final T defaultValue;

    protected Key(String name, Class<T> valueClass, @Nullable T defaultValue) {
        this.name = name;
        this.valueClass = valueClass;
        this.defaultValue = defaultValue;
    }

    @Override
    public final String toString() {
        return JdbdStrings.builder()
                .append(getClass().getName())
                .append("[ name : ")
                .append(this.name)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" , valueClass : ")
                .append(this.valueClass.getName())
                .append(" , defaultValue : ")
                .append(this.defaultValue)
                .append(" ]")
                .toString();
    }


    @SuppressWarnings("unchecked")
    protected static <T, K extends Key<T>> void addAllKey(final Class<K> keyClass, final Consumer<K> consumer)
            throws IllegalAccessException {
        int modifier;
        for (Field field : keyClass.getDeclaredFields()) {
            modifier = field.getModifiers();
            if (keyClass.isAssignableFrom(field.getType())
                    && Modifier.isPublic(modifier)
                    && Modifier.isStatic(modifier)
                    && Modifier.isFinal(modifier)) {
                consumer.accept((K) field.get(null));
            }

        }

    }

}
