package io.jdbd.vendor.stmt;

import io.jdbd.lang.Nullable;

public interface ParamValue {

    int getParamIndex();

    boolean isLongData();

    @Nullable
    Object getValue();

    Object getNonNullValue() throws NullPointerException;

}
