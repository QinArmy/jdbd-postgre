package io.jdbd.vendor.util;

import io.qinarmy.util.ArrayUtils;
import io.qinarmy.util.Pair;

import java.lang.reflect.Array;

public abstract class JdbdArrays extends ArrayUtils {

    protected JdbdArrays() {
        throw new UnsupportedOperationException();
    }


    public static Object createArrayInstance(final Class<?> componentClass, final int dimension, final int length) {
        if (componentClass.isArray()) {
            throw new IllegalArgumentException("targetArrayClass error.");
        }
        final Class<?> componentType;
        if (dimension == 1) {
            componentType = componentClass;
        } else if (dimension > 1) {
            try {
                componentType = Class.forName(obtainComponentClassName(componentClass, dimension));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        } else {
            throw new IllegalArgumentException(String.format("dimension[%s] less than 1", dimension));
        }
        return Array.newInstance(componentType, length);
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

    /**
     * <p>
     * Get component class and dimension of array.
     * </p>
     *
     * @param arrayClass class of Array.
     * @return pair, first: component class,second,dimension of array.
     */
    public static Pair<Class<?>, Integer> getArrayDimensions(final Class<?> arrayClass) {
        if (!arrayClass.isArray()) {
            throw new IllegalArgumentException(String.format("%s isn't Array type.", arrayClass.getName()));
        }
        Class<?> componentClass = arrayClass.getComponentType();
        int dimensions = 1;
        while (componentClass.isArray()) {
            dimensions++;
            componentClass = componentClass.getComponentType();

        }
        return new Pair<>(componentClass, dimensions);
    }


}
