package io.jdbd.vendor.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.vendor.stmt.ParamValue;
import org.qinarmy.util.Pair;
import reactor.util.annotation.Nullable;

import java.util.Comparator;
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


    @Nullable
    public static JdbdSQLException sortAndCheckParamGroup(final int groupIndex
            , final List<? extends ParamValue> paramGroup) {

        paramGroup.sort(Comparator.comparingInt(ParamValue::getParamIndex));

        JdbdSQLException error = null;
        final int size = paramGroup.size();
        for (int i = 0, index; i < size; i++) {
            index = paramGroup.get(i).getParamIndex();
            if (index == i) {
                continue;
            }

            if (index < i) {
                error = JdbdExceptions.duplicationParameter(groupIndex, index);
            } else {
                error = JdbdExceptions.noParameterValue(groupIndex, i);
            }
            break;
        }
        return error;
    }
}
