package io.jdbd.postgre.util;

import io.jdbd.vendor.util.JdbdArrays;
import io.qinarmy.util.Pair;

public abstract class PgArrays extends JdbdArrays {


    public static Pair<Class<?>, Integer> getPgArrayType(Class<?> arrayClass) {
        final Pair<Class<?>, Integer> pair = getArrayDimensions(arrayClass);
        final Pair<Class<?>, Integer> typePair;
        if (pair.getFirst() == byte.class) {
            if (pair.getSecond() < 2) {
                String m = String.format("arrayClass[%s] isn't supported.", arrayClass.getName());
                throw new IllegalArgumentException(m);
            }
            typePair = new Pair<>(byte[].class, pair.getSecond() - 1);
        } else {
            typePair = pair;
        }
        return typePair;
    }


}
