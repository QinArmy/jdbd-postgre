package io.jdbd.type;

import io.jdbd.lang.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * @see CodeEnum
 */
@Deprecated
abstract class CodeEnumHolder {

    private static final ConcurrentMap<Class<?>, Map<Integer, ? extends CodeEnum>> CODE_MAP_HOLDER =
            new ConcurrentHashMap<>();

    protected CodeEnumHolder() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return a unmodifiable map.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    protected static <T extends Enum<T> & CodeEnum> Map<Integer, T> getMap(Class<T> clazz) {
        return (Map<Integer, T>) CODE_MAP_HOLDER.get(clazz);
    }

    /**
     * @see CodeEnum#getCodeMap(Class)
     */
    static <T extends Enum<T> & CodeEnum> Map<Integer, T> getCodeMap(Class<T> clazz)
            throws IllegalArgumentException {
        assertCodeEnum(clazz);

        Map<Integer, T> map = getMap(clazz);

        if (map == null) {
            T[] types = clazz.getEnumConstants();
            map = new HashMap<>((int) (types.length / 0.75f));

            for (T type : types) {
                if (map.putIfAbsent(type.code(), type) != null) {
                    String message = String.format("Enum[%s] code[%s]duplicate", clazz.getName(), type.code());
                    throw new IllegalArgumentException(message);
                }
            }
            map = Collections.unmodifiableMap(map);
            CODE_MAP_HOLDER.putIfAbsent(clazz, map);
        }
        return map;
    }

    private static void assertCodeEnum(Class<? extends CodeEnum> clazz) throws IllegalArgumentException {

        for (Field f : clazz.getDeclaredFields()) {
            if (!Modifier.isFinal(f.getModifiers())) {
                String message = String.format("CodeEnum property[%s.%s]  properties must final.",
                        clazz.getName(),
                        f.getName());
                throw new IllegalArgumentException(message);
            }

        }

    }


}
