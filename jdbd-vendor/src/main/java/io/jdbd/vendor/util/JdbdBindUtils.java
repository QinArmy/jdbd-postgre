package io.jdbd.vendor.util;

import io.jdbd.vendor.IBindValue;

import java.util.List;

public abstract class JdbdBindUtils {

    protected JdbdBindUtils() {
        throw new UnsupportedOperationException();
    }


    public static boolean hasLongData(List<? extends IBindValue> parameterGroup) {
        boolean has = false;
        for (IBindValue bindValue : parameterGroup) {
            if (bindValue.isLongData()) {
                has = true;
                break;
            }
        }
        return has;
    }

    public static <T extends IBindValue> boolean hasLongDataGroup(List<List<T>> parameterGroupList) {
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
