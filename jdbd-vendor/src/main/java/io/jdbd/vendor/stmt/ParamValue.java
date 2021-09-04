package io.jdbd.vendor.stmt;

import io.jdbd.lang.Nullable;

public interface ParamValue {

    int getParamIndex();

    default boolean isLongData() {
        return false;
    }

    @Nullable
    Object getValue();

    Object getNonNullValue() throws NullPointerException;

}
