package io.jdbd.vendor.util;

import org.qinarmy.util.ArrayUtils;

import java.lang.reflect.Array;

public abstract class JdbdArrays extends ArrayUtils {

    protected JdbdArrays() {
        throw new UnsupportedOperationException();
    }


    public static Object createArrayInstance(final Class<?> targetArrayClass, final int dimension, final int length) {
        if (targetArrayClass.isArray()) {
            throw new IllegalArgumentException("targetArrayClass error.");
        }
        final Class<?> componentClass;
        if (dimension == 1) {
            componentClass = targetArrayClass;
        } else if (dimension > 1) {
            try {
                componentClass = Class.forName(obtainComponentClassName(targetArrayClass, dimension));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        } else {
            throw new IllegalArgumentException(String.format("dimension[%s] less than 1", dimension));
        }
        return Array.newInstance(componentClass, length);
    }

    private static String obtainComponentClassName(final Class<?> targetArrayClass, final int dimension) {
        final String className;
        if (targetArrayClass.isPrimitive()) {
            if (targetArrayClass == int.class) {
                className = "I";
            } else if (targetArrayClass == long.class) {
                className = "J";
            } else if (targetArrayClass == short.class) {
                className = "S";
            } else if (targetArrayClass == byte.class) {
                className = "B";
            } else if (targetArrayClass == boolean.class) {
                className = "Z";
            } else if (targetArrayClass == float.class) {
                className = "F";
            } else if (targetArrayClass == double.class) {
                className = "D";
            } else if (targetArrayClass == char.class) {
                className = "C";
            } else {
                String m = String.format("targetArrayClass[%s] not supported", targetArrayClass.getName());
                throw new IllegalArgumentException(m);
            }
        } else {
            className = targetArrayClass.getName();
        }


        final StringBuilder builder = new StringBuilder(dimension + 2 + className.length());
        // i = 1 not i = 0, because Array.newInstance need componentType
        for (int i = 1; i < dimension; i++) {
            builder.append('[');
        }
        final boolean appendL = !targetArrayClass.isPrimitive();
        if (appendL) {
            builder.append('L');
        }
        builder.append(className);
        if (appendL) {
            builder.append(';');
        }
        return builder.toString();
    }

}
