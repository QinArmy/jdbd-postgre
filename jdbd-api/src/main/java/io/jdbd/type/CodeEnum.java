package io.jdbd.type;

import io.jdbd.lang.Nullable;

import java.util.Map;


/**
 * <p>
 * see Book Effective Java item (Use instance fields instead of ordinals).
 * </p>
 * <p>
 * Code should isn't consecutive numbers,like 0,1,2,3.... because you should consider add new enum instance.
 * code should like below :
 *      <ul>
 *          <li>0</li>
 *          <li>100</li>
 *          <li>200</li>
 *          <li>300</li>
 *      </ul>
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
        return getCodeMap((Class<T>) enumClass).get(code);
    }

    static <T extends Enum<T> & CodeEnum> Map<Integer, T> getCodeMap(Class<T> clazz)
            throws IllegalArgumentException {
        return CodeEnumHolder.getCodeMap(clazz);
    }

}
