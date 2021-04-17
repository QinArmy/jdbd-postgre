package io.jdbd.vendor.util;

import io.jdbd.vendor.stmt.ParamValue;

import java.util.List;

public abstract class JdbdBindUtils {

    protected JdbdBindUtils() {
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

    public static <T extends ParamValue> boolean hasLongDataGroup(List<List<T>> parameterGroupList) {
        boolean has = false;
        for (List<T> list : parameterGroupList) {
            if (hasLongData(list)) {
                has = true;
                break;
            }
        }
        return has;
    }
}
