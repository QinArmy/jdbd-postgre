package io.jdbd.vendor.util;

import io.jdbd.vendor.stmt.ParamValue;
import org.qinarmy.util.Pair;

import java.util.List;

public abstract class JdbdBinds {

    protected JdbdBinds() {
        throw new UnsupportedOperationException();
    }


    public static boolean hasLongData(List<? extends ParamValue> parameterGroup) {
        boolean has = false;
        for (ParamValue bindValue : parameterGroup) {
            if (bindValue.isLongData()) {
                has = true;
                break;
            }
        }
        return has;
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
